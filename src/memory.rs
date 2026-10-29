//! The memory budget: one number the operator sets, split across everything the
//! server holds in memory.
//!
//! `CAREGRAPH_MEMORY_MB` is the budget for the **whole server process tree**: the
//! Rust server and its Python embedding workers. Each worker is a separate
//! process that imports `torch` and `torch_geometric` (about 330 MB resident
//! before it does any work, measured; see `docs/memory.md`), so the budget first
//! reserves that for every worker that will run, then divides what is left among
//! the parts of the server that grow:
//!
//! | Component | Share of the remainder `A` | Bounded by |
//! |---|---|---|
//! | RocksDB: block cache, index/filter blocks and memtables | 80% | one cache shared by every database in the process; memtables are charged to it |
//! | node-type cache | 10% | an entry cap |
//! | recent-batch tail for replication | 10% (4-32 MB) | a byte cap |
//!
//! `A` is the budget minus the workers, minus a fixed 80 MB for code, threads and
//! the runtime, minus 10% headroom for requests in flight. Without a budget the
//! historical defaults apply (256 MB RocksDB cache, 64 MB memtables per column
//! family, 4 M node-type entries, 32 MB tail) and nothing is capped beyond them.
//!
//! Any component can still be set by hand and then wins over the derived value:
//! `CAREGRAPH_BLOCK_CACHE_MB` (the RocksDB cache), `CAREGRAPH_WRITE_BUFFER_MB`
//! (one memtable), `CAREGRAPH_MEMTABLE_TOTAL_MB` (all memtables together),
//! `CAREGRAPH_NODE_TYPE_CACHE_ENTRIES`, `CAREGRAPH_RECENT_TAIL_MB`.
//!
//! What the budget does **not** do: it does not limit a Python worker's own
//! growth (that is bounded by how many workers run, and a worker grew by about
//! 70 MB under a write load in the measurements), and it cannot make a request
//! that needs more memory than the headroom fit. It is a plan that is enforced
//! where the server controls the allocation, exported as metrics so the rest can
//! be watched, and refused at startup when it cannot be met.

use std::sync::OnceLock;

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGauge, IntGaugeVec, Opts};
use rocksdb::{Cache, WriteBufferManager};

/// Resident memory of one embedding worker with `torch` and `torch_geometric`
/// imported and a model loaded: 330 MB measured idle, 402 MB after a write load;
/// rounded up.
pub const DEFAULT_WORKER_MB: u64 = 450;

/// Code, thread stacks, the async runtime, connection state.
const FIXED_OVERHEAD_MB: u64 = 80;
/// The least RocksDB can usefully run with.
const MIN_ROCKSDB_MB: u64 = 32;
/// Bytes charged per node-type cache entry (key, value, hash table slack).
const NODE_ENTRY_BYTES: u64 = 48;

const LEGACY_ROCKSDB_MB: u64 = 256;
const LEGACY_WRITE_BUFFER_MB: u64 = 64;
const LEGACY_NODE_ENTRIES: u64 = 4_000_000;
const LEGACY_TAIL_MB: u64 = 32;

/// Components set by hand; each wins over its derived value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overrides {
    pub rocksdb_mb: Option<u64>,
    pub write_buffer_mb: Option<u64>,
    pub memtable_total_mb: Option<u64>,
    pub node_type_entries: Option<u64>,
    pub recent_tail_mb: Option<u64>,
}

impl Overrides {
    pub fn from_env() -> Self {
        let n = |name: &str| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|&v| v > 0)
        };
        Overrides {
            rocksdb_mb: n("CAREGRAPH_BLOCK_CACHE_MB"),
            write_buffer_mb: n("CAREGRAPH_WRITE_BUFFER_MB"),
            memtable_total_mb: n("CAREGRAPH_MEMTABLE_TOTAL_MB"),
            node_type_entries: n("CAREGRAPH_NODE_TYPE_CACHE_ENTRIES"),
            recent_tail_mb: n("CAREGRAPH_RECENT_TAIL_MB"),
        }
    }
}

/// What the plan is derived from.
#[derive(Debug, Clone)]
pub struct Inputs {
    /// `CAREGRAPH_MEMORY_MB`; `None` keeps the historical defaults.
    pub budget_mb: Option<u64>,
    /// Embedding model pools that will run (deployed models).
    pub pools: usize,
    pub workers_per_pool: usize,
    /// Expected resident memory per worker.
    pub worker_mb: u64,
    pub overrides: Overrides,
}

impl Default for Inputs {
    fn default() -> Self {
        Inputs {
            budget_mb: None,
            pools: 0,
            workers_per_pool: 1,
            worker_mb: DEFAULT_WORKER_MB,
            overrides: Overrides::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPlan {
    pub budget_mb: Option<u64>,
    /// Capacity of the shared RocksDB cache (blocks, index, filters, memtables).
    pub rocksdb_mb: u64,
    /// Limit on all memtables together, charged to the shared cache. `None`
    /// leaves memtables bounded only by their per-column-family size.
    pub memtable_total_mb: Option<u64>,
    /// Size of one memtable.
    pub write_buffer_mb: u64,
    pub node_type_entries: u64,
    pub recent_tail_mb: u64,
    /// RocksDB flush/compaction threads.
    pub background_jobs: usize,
    pub pools: usize,
    pub workers_per_pool: usize,
    pub worker_mb: u64,
    /// Reserved for the workers (`pools * workers_per_pool * worker_mb`).
    pub workers_mb: u64,
    /// Headroom for requests in flight (zero without a budget).
    pub request_mb: u64,
}

impl MemoryPlan {
    /// The plan for `inputs`, or the reason it cannot be met.
    pub fn derive(inputs: &Inputs, cpus: usize) -> Result<MemoryPlan, String> {
        let o = &inputs.overrides;
        let workers_mb = inputs.pools as u64 * inputs.workers_per_pool as u64 * inputs.worker_mb;
        let Some(budget) = inputs.budget_mb else {
            return Ok(MemoryPlan {
                budget_mb: None,
                rocksdb_mb: o.rocksdb_mb.unwrap_or(LEGACY_ROCKSDB_MB),
                memtable_total_mb: o.memtable_total_mb,
                write_buffer_mb: o.write_buffer_mb.unwrap_or(LEGACY_WRITE_BUFFER_MB),
                node_type_entries: o.node_type_entries.unwrap_or(LEGACY_NODE_ENTRIES),
                recent_tail_mb: o.recent_tail_mb.unwrap_or(LEGACY_TAIL_MB),
                background_jobs: cpus.max(2),
                pools: inputs.pools,
                workers_per_pool: inputs.workers_per_pool,
                worker_mb: inputs.worker_mb,
                workers_mb,
                request_mb: 0,
            });
        };

        let floor = workers_mb + FIXED_OVERHEAD_MB + MIN_ROCKSDB_MB;
        if budget < floor {
            return Err(format!(
                "CAREGRAPH_MEMORY_MB={budget} is too small: {} embedding worker(s) at {} MB each \
                 ({workers_mb} MB), {FIXED_OVERHEAD_MB} MB for the runtime and at least \
                 {MIN_ROCKSDB_MB} MB for the database need {floor} MB. Raise the budget, or run \
                 fewer workers (CAREGRAPH_WORKERS) or models (CAREGRAPH_GAT_MODEL=none, \
                 CAREGRAPH_GRAPHSAGE_MODEL=none): each worker costs about {} MB.",
                inputs.pools * inputs.workers_per_pool,
                inputs.worker_mb,
                inputs.worker_mb
            ));
        }
        let remainder = budget - workers_mb - FIXED_OVERHEAD_MB;
        let request_mb = (remainder / 10).max(16).min(remainder / 2);
        let a = remainder - request_mb;

        let rocksdb_mb = o.rocksdb_mb.unwrap_or((a * 8 / 10).max(MIN_ROCKSDB_MB));
        let memtable_total_mb = o
            .memtable_total_mb
            .unwrap_or((rocksdb_mb * 3 / 10).max(8))
            .min(rocksdb_mb);
        let write_buffer_mb = o
            .write_buffer_mb
            .unwrap_or((memtable_total_mb / 4).clamp(4, LEGACY_WRITE_BUFFER_MB));
        let node_bytes = (a / 10).max(4) << 20;
        let node_type_entries = o
            .node_type_entries
            .unwrap_or((node_bytes / NODE_ENTRY_BYTES).max(10_000));
        let recent_tail_mb = o
            .recent_tail_mb
            .unwrap_or((a / 10).clamp(4, LEGACY_TAIL_MB));
        // Compaction and flush buffers scale with the job count, so a small
        // budget runs fewer of them.
        let background_jobs = ((a / 256) as usize).clamp(2, cpus.max(2));

        Ok(MemoryPlan {
            budget_mb: Some(budget),
            rocksdb_mb,
            memtable_total_mb: Some(memtable_total_mb),
            write_buffer_mb,
            node_type_entries,
            recent_tail_mb,
            background_jobs,
            pools: inputs.pools,
            workers_per_pool: inputs.workers_per_pool,
            worker_mb: inputs.worker_mb,
            workers_mb,
            request_mb,
        })
    }

    /// Bytes the plan accounts for, by component, for metrics and the startup
    /// log. The shared RocksDB cache already contains the memtables.
    pub fn components(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("rocksdb", self.rocksdb_mb << 20),
            (
                "node_type_cache",
                self.node_type_entries.saturating_mul(NODE_ENTRY_BYTES),
            ),
            ("recent_tail", self.recent_tail_mb << 20),
            ("workers", self.workers_mb << 20),
            ("request_headroom", self.request_mb << 20),
            ("runtime", FIXED_OVERHEAD_MB << 20),
        ]
    }

    /// Planned total in MB: what the components add up to.
    pub fn planned_mb(&self) -> u64 {
        self.components().iter().map(|(_, b)| b >> 20).sum()
    }

    pub fn describe(&self) -> String {
        let mut out = match self.budget_mb {
            Some(b) => format!("memory budget {b} MB (whole process tree)\n"),
            None => "memory budget not set: historical defaults, nothing capped beyond them\n"
                .to_string(),
        };
        out.push_str(&format!(
            "  embedding workers   {:>6} MB  ({} pool(s) x {} worker(s) x {} MB)\n",
            self.workers_mb, self.pools, self.workers_per_pool, self.worker_mb
        ));
        out.push_str(&format!(
            "  RocksDB (shared)    {:>6} MB  cache, index/filters and memtables; memtables {} per column family, {} in total\n",
            self.rocksdb_mb,
            format_args!("{} MB", self.write_buffer_mb),
            match self.memtable_total_mb {
                Some(t) => format!("{t} MB"),
                None => "unlimited".to_string(),
            }
        ));
        out.push_str(&format!(
            "  node-type cache     {:>6} MB  ({} entries)\n",
            (self.node_type_entries * NODE_ENTRY_BYTES) >> 20,
            self.node_type_entries
        ));
        out.push_str(&format!(
            "  replication tail    {:>6} MB\n",
            self.recent_tail_mb
        ));
        out.push_str(&format!(
            "  request headroom    {:>6} MB\n  runtime             {:>6} MB\n",
            self.request_mb, FIXED_OVERHEAD_MB
        ));
        out.push_str(&format!(
            "  RocksDB threads     {:>6}\n",
            self.background_jobs
        ));
        out
    }
}

static PLAN: OnceLock<MemoryPlan> = OnceLock::new();
static CACHE: OnceLock<Cache> = OnceLock::new();
static MEMTABLES: OnceLock<Option<WriteBufferManager>> = OnceLock::new();

/// Installs the plan the process will run with. Must happen before the first
/// database is opened; later calls (and plans for tests) are ignored.
pub fn install(plan: MemoryPlan) -> &'static MemoryPlan {
    PLAN.get_or_init(|| plan)
}

/// The installed plan, or the historical defaults (with any per-component
/// environment overrides) when none was installed: tests, benchmarks and the
/// admin tools run this way.
pub fn current() -> &'static MemoryPlan {
    PLAN.get_or_init(|| {
        MemoryPlan::derive(
            &Inputs {
                overrides: Overrides::from_env(),
                ..Inputs::default()
            },
            std::thread::available_parallelism().map_or(2, |n| n.get()),
        )
        .expect("a plan without a budget cannot fail")
    })
}

/// The one cache every database in this process shares: the block cache, with
/// index and filter blocks, and (through the write buffer manager) memtables.
pub fn rocksdb_cache() -> &'static Cache {
    CACHE.get_or_init(|| Cache::new_lru_cache((current().rocksdb_mb as usize) << 20))
}

/// Caps all memtables in the process together and charges them to
/// [`rocksdb_cache`]; `None` when no total was requested.
pub fn memtable_manager() -> Option<&'static WriteBufferManager> {
    MEMTABLES
        .get_or_init(|| {
            current().memtable_total_mb.map(|mb| {
                WriteBufferManager::new_write_buffer_manager_with_cache(
                    (mb as usize) << 20,
                    false,
                    rocksdb_cache().clone(),
                )
            })
        })
        .as_ref()
}

/// Resident memory of process `pid`, if it can be read.
#[cfg(windows)]
pub fn resident_bytes(pid: u32) -> Option<u64> {
    use std::ffi::c_void;

    #[repr(C)]
    struct Counters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set: usize,
        working_set: usize,
        rest: [usize; 6],
    }
    extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn K32GetProcessMemoryInfo(handle: *mut c_void, counters: *mut Counters, cb: u32) -> i32;
    }
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    // SAFETY: plain Win32 calls with a handle we open and close here and a
    // correctly sized, zero-initialised output structure.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return None;
        }
        let mut c = Counters {
            cb: std::mem::size_of::<Counters>() as u32,
            page_fault_count: 0,
            peak_working_set: 0,
            working_set: 0,
            rest: [0; 6],
        };
        let ok = K32GetProcessMemoryInfo(handle, &mut c, c.cb);
        CloseHandle(handle);
        (ok != 0).then_some(c.working_set as u64)
    }
}

#[cfg(target_os = "linux")]
pub fn resident_bytes(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb * 1024)
}

#[cfg(not(any(windows, target_os = "linux")))]
pub fn resident_bytes(_pid: u32) -> Option<u64> {
    None
}

/// `pid` and every process descended from it.
#[cfg(windows)]
pub fn process_tree(pid: u32) -> Vec<u32> {
    use std::collections::HashMap;
    use std::ffi::c_void;

    #[repr(C)]
    struct Entry {
        size: u32,
        usage: u32,
        pid: u32,
        heap: usize,
        module: u32,
        threads: u32,
        parent: u32,
        priority: i32,
        flags: u32,
        exe: [u16; 260],
    }
    extern "system" {
        fn CreateToolhelp32Snapshot(flags: u32, pid: u32) -> *mut c_void;
        fn Process32FirstW(snapshot: *mut c_void, entry: *mut Entry) -> i32;
        fn Process32NextW(snapshot: *mut c_void, entry: *mut Entry) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }
    const TH32CS_SNAPPROCESS: u32 = 2;
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    // SAFETY: a process snapshot handle we create and close here, and an entry
    // structure with its size field set as the API requires.
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if snapshot as usize == usize::MAX || snapshot.is_null() {
            return vec![pid];
        }
        let mut entry = Entry {
            size: std::mem::size_of::<Entry>() as u32,
            usage: 0,
            pid: 0,
            heap: 0,
            module: 0,
            threads: 0,
            parent: 0,
            priority: 0,
            flags: 0,
            exe: [0; 260],
        };
        let mut more = Process32FirstW(snapshot, &mut entry) != 0;
        while more {
            children.entry(entry.parent).or_default().push(entry.pid);
            more = Process32NextW(snapshot, &mut entry) != 0;
        }
        CloseHandle(snapshot);
    }
    collect_tree(pid, &children)
}

#[cfg(target_os = "linux")]
pub fn process_tree(pid: u32) -> Vec<u32> {
    use std::collections::HashMap;
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    if let Ok(dir) = std::fs::read_dir("/proc") {
        for entry in dir.flatten() {
            let Some(child) = entry
                .file_name()
                .to_str()
                .and_then(|n| n.parse::<u32>().ok())
            else {
                continue;
            };
            // `pid (comm) state ppid ...`; comm may contain spaces and parentheses.
            let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            let parent = stat
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().nth(1))
                .and_then(|p| p.parse::<u32>().ok());
            if let Some(parent) = parent {
                children.entry(parent).or_default().push(child);
            }
        }
    }
    collect_tree(pid, &children)
}

#[cfg(not(any(windows, target_os = "linux")))]
pub fn process_tree(pid: u32) -> Vec<u32> {
    vec![pid]
}

#[cfg(any(windows, target_os = "linux"))]
fn collect_tree(root: u32, children: &std::collections::HashMap<u32, Vec<u32>>) -> Vec<u32> {
    let mut out = vec![root];
    let mut i = 0;
    while i < out.len() {
        if let Some(kids) = children.get(&out[i]) {
            for k in kids {
                if !out.contains(k) {
                    out.push(*k);
                }
            }
        }
        i += 1;
    }
    out
}

/// Resident memory of `pid` and everything it started. A worker started through
/// a launcher (a Python virtual environment on Windows starts the real
/// interpreter as a child) is counted whole.
pub fn tree_resident_bytes(pid: u32) -> u64 {
    process_tree(pid)
        .into_iter()
        .filter_map(resident_bytes)
        .sum()
}

type WorkerPids = std::sync::Arc<dyn Fn() -> Vec<(String, Vec<u32>)> + Send + Sync>;

/// Exports the plan and what the process tree actually holds, read when
/// scraped: the budget, the plan by component, the server's own resident size,
/// each model's workers' resident size, and the total, so an alert can compare
/// actual against budget.
pub struct MemoryCollector {
    plan: MemoryPlan,
    workers: WorkerPids,
    budget: IntGauge,
    planned: IntGaugeVec,
    process: IntGauge,
    worker_rss: IntGaugeVec,
    tree: IntGauge,
}

impl MemoryCollector {
    pub fn register(
        plan: MemoryPlan,
        workers: WorkerPids,
        registry: &prometheus::Registry,
    ) -> prometheus::Result<()> {
        let c = MemoryCollector {
            plan,
            workers,
            budget: IntGauge::new(
                "memory_budget_bytes",
                "CAREGRAPH_MEMORY_MB for the whole process tree (0 when not set)",
            )?,
            planned: IntGaugeVec::new(
                Opts::new(
                    "memory_planned_bytes",
                    "Memory the plan gives each component",
                ),
                &["component"],
            )?,
            process: IntGauge::new(
                "process_resident_memory_bytes",
                "Resident memory of the server process itself",
            )?,
            worker_rss: IntGaugeVec::new(
                Opts::new(
                    "worker_resident_memory_bytes",
                    "Resident memory of a model's embedding workers, interpreters included",
                ),
                &["model"],
            )?,
            tree: IntGauge::new(
                "tree_resident_memory_bytes",
                "Resident memory of the server and its workers together",
            )?,
        };
        registry.register(Box::new(c))
    }

    fn refresh(&self) {
        self.budget
            .set(self.plan.budget_mb.map_or(0, |mb| (mb << 20) as i64));
        for (name, bytes) in self.plan.components() {
            self.planned.with_label_values(&[name]).set(bytes as i64);
        }
        let own = resident_bytes(std::process::id()).unwrap_or(0);
        self.process.set(own as i64);
        let mut total = own;
        for (model, pids) in (self.workers)() {
            let bytes: u64 = pids.into_iter().map(tree_resident_bytes).sum();
            self.worker_rss
                .with_label_values(&[&model])
                .set(bytes as i64);
            total += bytes;
        }
        self.tree.set(total as i64);
    }
}

impl Collector for MemoryCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut d = Vec::new();
        d.extend(self.budget.desc());
        d.extend(self.planned.desc());
        d.extend(self.process.desc());
        d.extend(self.worker_rss.desc());
        d.extend(self.tree.desc());
        d
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        let mut out = Vec::new();
        out.extend(self.budget.collect());
        out.extend(self.planned.collect());
        out.extend(self.process.collect());
        out.extend(self.worker_rss.collect());
        out.extend(self.tree.collect());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(budget: Option<u64>, pools: usize) -> Inputs {
        Inputs {
            budget_mb: budget,
            pools,
            ..Inputs::default()
        }
    }

    #[test]
    fn without_a_budget_the_historical_defaults_apply() {
        let p = MemoryPlan::derive(&inputs(None, 2), 8).unwrap();
        assert_eq!(
            (
                p.rocksdb_mb,
                p.write_buffer_mb,
                p.node_type_entries,
                p.recent_tail_mb
            ),
            (256, 64, 4_000_000, 32)
        );
        assert_eq!(p.memtable_total_mb, None);
        assert_eq!(p.workers_mb, 2 * DEFAULT_WORKER_MB);
    }

    #[test]
    fn a_budget_reserves_the_workers_first_and_the_parts_fit_inside_it() {
        for (budget, pools) in [(1024, 1), (2048, 2), (4096, 2), (8192, 2), (600, 0)] {
            let p = MemoryPlan::derive(&inputs(Some(budget), pools), 16).unwrap();
            assert_eq!(p.workers_mb, pools as u64 * DEFAULT_WORKER_MB);
            // The memtables are inside the RocksDB cache, not on top of it.
            assert!(p.memtable_total_mb.unwrap() <= p.rocksdb_mb, "{p:?}");
            assert!(
                p.planned_mb() <= budget,
                "budget {budget} MB with {pools} pool(s) plans {} MB:\n{}",
                p.planned_mb(),
                p.describe()
            );
            // And it is used, not left idle: at least 85% of the budget.
            assert!(p.planned_mb() * 100 >= budget * 85, "{}", p.describe());
        }
    }

    #[test]
    fn a_larger_budget_gives_every_growing_component_more() {
        let small = MemoryPlan::derive(&inputs(Some(1500), 1), 16).unwrap();
        let large = MemoryPlan::derive(&inputs(Some(6000), 1), 16).unwrap();
        assert!(large.rocksdb_mb > small.rocksdb_mb);
        assert!(large.node_type_entries > small.node_type_entries);
        assert!(large.write_buffer_mb >= small.write_buffer_mb);
        assert!(large.background_jobs >= small.background_jobs);
    }

    #[test]
    fn a_budget_that_cannot_hold_the_workers_is_refused_with_advice() {
        let err = MemoryPlan::derive(&inputs(Some(500), 2), 8).unwrap_err();
        assert!(err.contains("too small"), "{err}");
        assert!(err.contains("CAREGRAPH_WORKERS"), "{err}");
        assert!(err.contains("CAREGRAPH_GAT_MODEL=none"), "{err}");
        // The same budget is fine for a node with no workers (a reader).
        MemoryPlan::derive(&inputs(Some(500), 0), 8).unwrap();
        // Two pools of two workers cost four times a single worker.
        let mut i = inputs(Some(1900), 2);
        i.workers_per_pool = 2;
        assert!(MemoryPlan::derive(&i, 8).is_err());
    }

    #[test]
    fn a_component_set_by_hand_wins_over_the_derived_value() {
        let mut i = inputs(Some(4096), 1);
        i.overrides = Overrides {
            rocksdb_mb: Some(100),
            write_buffer_mb: Some(7),
            memtable_total_mb: Some(40),
            node_type_entries: Some(123_456),
            recent_tail_mb: Some(9),
        };
        let p = MemoryPlan::derive(&i, 8).unwrap();
        assert_eq!(
            (
                p.rocksdb_mb,
                p.write_buffer_mb,
                p.memtable_total_mb,
                p.node_type_entries,
                p.recent_tail_mb
            ),
            (100, 7, Some(40), 123_456, 9)
        );
    }

    #[test]
    fn the_background_thread_count_follows_the_budget_but_never_the_machine_alone() {
        let tiny = MemoryPlan::derive(&inputs(Some(700), 0), 32).unwrap();
        assert_eq!(tiny.background_jobs, 2);
        let big = MemoryPlan::derive(&inputs(Some(16_000), 0), 4).unwrap();
        assert_eq!(big.background_jobs, 4, "capped by the CPUs available");
    }

    #[test]
    fn the_process_tree_includes_children_and_grandchildren() {
        if !cfg!(any(windows, target_os = "linux")) {
            return;
        }
        let me = std::process::id();
        assert_eq!(process_tree(me)[0], me);
        // A shell that starts a second process: two descendants.
        let mut child = if cfg!(windows) {
            std::process::Command::new("cmd")
                .args(["/C", "ping -n 6 127.0.0.1 > NUL"])
                .spawn()
                .unwrap()
        } else {
            std::process::Command::new("sh")
                .args(["-c", "sleep 5; true"])
                .spawn()
                .unwrap()
        };
        std::thread::sleep(std::time::Duration::from_millis(500));
        let tree = process_tree(me);
        assert!(tree.contains(&child.id()), "{tree:?}");
        assert!(tree.len() >= 3, "child and its own child: {tree:?}");
        assert!(tree_resident_bytes(me) > resident_bytes(me).unwrap());
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn this_process_can_read_its_own_resident_size() {
        let bytes = resident_bytes(std::process::id());
        if cfg!(any(windows, target_os = "linux")) {
            assert!(bytes.unwrap() > 1 << 20, "{bytes:?}");
        }
    }
}
