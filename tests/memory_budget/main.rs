//! Does the memory budget hold?
//!
//! A write-then-read workload that touches far more data than any cache should
//! keep runs in two child processes: once with the historical defaults and once
//! with `CAREGRAPH_MEMORY_MB=256`. The memory plan is process-wide and must be
//! installed before the first database opens, which is why each run is its own
//! process (and why this is its own test target). The budgeted run must stay
//! inside its budget and well below the unbudgeted one, and the shared memtable
//! limit must actually be in force.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use caregraph::memory::{self, Inputs, MemoryPlan};
use caregraph::storage::{cf, KvStore, RocksKv};
use rocksdb::WriteBatch;

const VALUES: u64 = 200_000;
const VALUE_BYTES: usize = 2_000;
const BUDGET_MB: u64 = 256;

/// Deterministic incompressible bytes (xorshift), so compression cannot shrink
/// the data below what the cache has to hold.
fn bytes(seed: u64, out: &mut Vec<u8>) {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    out.clear();
    while out.len() < VALUE_BYTES {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(VALUE_BYTES);
}

/// Writes 400 MB across two column families, then reads it back at random and
/// scans it, sampling this process's resident memory the whole time. Prints the
/// peak so the parent can read it.
#[test]
#[ignore = "run by the_budget_bounds_memory_a_workload_would_otherwise_use"]
fn child_workload() {
    let budgeted = std::env::var("CG_MEMORY_MODE").is_ok_and(|m| m == "budget");
    if budgeted {
        let plan = MemoryPlan::derive(
            &Inputs {
                budget_mb: Some(BUDGET_MB),
                pools: 0,
                ..Inputs::default()
            },
            8,
        )
        .unwrap();
        memory::install(plan);
    }

    let peak = Arc::new(AtomicU64::new(0));
    let memtable_peak = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, memtable_peak, stop) = (peak.clone(), memtable_peak.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Some(b) = memory::resident_bytes(std::process::id()) {
                    peak.fetch_max(b, Ordering::Relaxed);
                }
                if let Some(m) = memory::memtable_manager() {
                    memtable_peak.fetch_max(m.get_usage() as u64, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(40));
            }
        })
    };

    let dir = std::env::var("CG_MEMORY_DIR").expect("CG_MEMORY_DIR");
    let store = RocksKv::open(dir).unwrap();
    let nodes = store.cf_handle(cf::CF_NODES).unwrap();
    let embeddings = store.cf_handle(cf::CF_EMBEDDINGS).unwrap();
    let mut value = Vec::new();
    let mut batch = WriteBatch::default();
    for i in 0..VALUES {
        bytes(i, &mut value);
        let key = i.to_be_bytes();
        // Alternate families so more than one memtable is active at a time.
        batch.put_cf(
            if i % 2 == 0 { &nodes } else { &embeddings },
            [&key[..], &[0u8; 8]].concat(),
            &value,
        );
        if i % 500 == 499 {
            store.write(std::mem::take(&mut batch)).unwrap();
        }
    }
    store.write(batch).unwrap();
    store.flush(cf::CF_NODES).unwrap();
    store.flush(cf::CF_EMBEDDINGS).unwrap();

    // Random point reads, then a full scan: both go through the cache.
    let mut x = 12345u64;
    let mut hits = 0u64;
    for _ in 0..VALUES {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let i = x % VALUES;
        let cf_name = if i % 2 == 0 {
            cf::CF_NODES
        } else {
            cf::CF_EMBEDDINGS
        };
        if store
            .get(cf_name, &[&i.to_be_bytes()[..], &[0u8; 8]].concat())
            .unwrap()
            .is_some()
        {
            hits += 1;
        }
    }
    assert_eq!(hits, VALUES, "every key was written and must be found");
    let mut scanned = 0u64;
    store
        .scan_from_narrow_prefix(cf::CF_NODES, &[], &[], &mut |_, _| {
            scanned += 1;
            std::ops::ControlFlow::Continue(())
        })
        .unwrap();
    assert_eq!(scanned, VALUES / 2);

    stop.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    let memtable_mb = memtable_peak.load(Ordering::Relaxed) >> 20;
    println!("PEAK_RSS_MB={}", peak.load(Ordering::Relaxed) >> 20);
    println!("MEMTABLE_USAGE_MB={memtable_mb}");
    println!(
        "MEMTABLE_LIMIT_MB={}",
        memory::current().memtable_total_mb.unwrap_or(0)
    );
}

fn run_child(mode: &str) -> std::collections::HashMap<String, u64> {
    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["child_workload", "--ignored", "--exact", "--nocapture"])
        .env("CG_MEMORY_MODE", mode)
        .env("CG_MEMORY_DIR", dir.path())
        .env_remove("CAREGRAPH_BLOCK_CACHE_MB")
        .env_remove("CAREGRAPH_WRITE_BUFFER_MB")
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{mode} child failed:\n{text}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    text.lines()
        .filter_map(|l| l.split_once('='))
        .filter_map(|(k, v)| Some((k.to_string(), v.trim().parse().ok()?)))
        .collect()
}

#[test]
fn the_budget_bounds_memory_a_workload_would_otherwise_use() {
    let free = run_child("default");
    let bounded = run_child("budget");
    let (free_mb, bounded_mb) = (free["PEAK_RSS_MB"], bounded["PEAK_RSS_MB"]);
    eprintln!(
        "peak resident memory: {free_mb} MB with the historical defaults, {bounded_mb} MB with \
         CAREGRAPH_MEMORY_MB={BUDGET_MB}; memtables peaked at {} MB against a {} MB limit",
        bounded["MEMTABLE_USAGE_MB"], bounded["MEMTABLE_LIMIT_MB"]
    );
    assert!(
        bounded_mb <= BUDGET_MB,
        "the budgeted run used {bounded_mb} MB, over its {BUDGET_MB} MB budget"
    );
    assert!(
        bounded_mb * 100 <= free_mb * 75,
        "the budget saved too little: {bounded_mb} MB against {free_mb} MB unbudgeted"
    );
    // Memtables stay near their limit (RocksDB flushes at ~90% of it and a batch
    // in flight can overshoot a little).
    assert!(
        bounded["MEMTABLE_USAGE_MB"] > 0,
        "the sampler never saw the memtables in use, so the limit was not exercised"
    );
    assert!(
        bounded["MEMTABLE_USAGE_MB"] <= bounded["MEMTABLE_LIMIT_MB"] * 3 / 2 + 8,
        "memtables peaked at {} MB against a {} MB limit",
        bounded["MEMTABLE_USAGE_MB"],
        bounded["MEMTABLE_LIMIT_MB"]
    );
    assert_eq!(free["MEMTABLE_LIMIT_MB"], 0, "no budget, no memtable limit");
}
