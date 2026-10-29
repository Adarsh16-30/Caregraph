# Memory

How much memory CareGraph uses, where it goes, and how to set it yourself. Every
number here was measured on one Windows 11 laptop (16 logical CPUs, 16 GB, with
roughly 9 GB already used by other programs) and is backed by a file under
`benchmarks/results/gate/`. They are measurements of this graph (the 174,298-node
Diabetes 130 graph, a 40 MB database) on this machine, not guarantees for yours.

## Setting the budget

One number covers the whole server: the Rust process **and** its Python embedding
workers.

```
CAREGRAPH_MEMORY_MB=1300
```

Set it in the environment, in `.env` (a commented example is there), or in the TOML
config. Unset, the server keeps the historical defaults and caps nothing beyond them.
`caregraph --print-config` prints the plan the budget produces without starting the
server:

```
memory budget 1300 MB (whole process tree)
  embedding workers      900 MB  (2 pool(s) x 1 worker(s) x 450 MB)
  RocksDB (shared)       230 MB  cache, index/filters and memtables; memtables 17 MB per column family, 69 MB in total
  node-type cache         27 MB  (611669 entries)
  replication tail        28 MB
  request headroom        32 MB
  runtime                 80 MB
  RocksDB threads          2
```

How it is divided:

1. **Workers first.** Every deployed model runs `CAREGRAPH_WORKERS` Python processes
   (default 1), each `CAREGRAPH_WORKER_MEMORY_MB` (default 450, see below). That
   comes off the top.
2. A fixed **80 MB** for code, thread stacks and the runtime, and **10%** of the rest
   as headroom for requests in flight.
3. What is left, `A`, is split: **80%** to RocksDB (one cache shared by every database
   in the process, holding data blocks, index and filter blocks and, charged to it,
   the memtables), **10%** to the node-type cache (an entry cap), **10%** to the
   replication tail (4 to 32 MB).

A budget that cannot hold the workers is **refused at startup** with what to change,
instead of running and swapping:

```
CAREGRAPH_MEMORY_MB=700 is too small: 2 embedding worker(s) at 450 MB each (900 MB),
80 MB for the runtime and at least 32 MB for the database need 1012 MB. Raise the
budget, or run fewer workers (CAREGRAPH_WORKERS) or models (CAREGRAPH_GAT_MODEL=none,
CAREGRAPH_GRAPHSAGE_MODEL=none): each worker costs about 450 MB.
```

Any single component can be set by hand and then wins over its derived value:

| Variable | Sets |
|---|---|
| `CAREGRAPH_BLOCK_CACHE_MB` | the shared RocksDB cache |
| `CAREGRAPH_MEMTABLE_TOTAL_MB` | all memtables together (a hard limit, charged to the cache) |
| `CAREGRAPH_WRITE_BUFFER_MB` | one memtable |
| `CAREGRAPH_NODE_TYPE_CACHE_ENTRIES` | node-type cache entries (about 48 bytes each) |
| `CAREGRAPH_RECENT_TAIL_MB` | batches kept in memory for replication |

### What is enforced and what is not

**Enforced** (the server controls the allocation): the RocksDB cache capacity, the
total memtable memory, the node-type cache, the replication tail, and the number of
RocksDB background threads. Startup fails if the budget cannot hold the workers.

**Not enforced:** a Python worker's own growth (it grew by about 70 MB under a write
load in the measurements; the number of workers is what bounds it) and any single
request that needs more than the headroom. The budget is a plan plus controls where
the server owns the memory, not an operating-system limit on the process. To watch
the rest, `/metrics` exports `memory_budget_bytes`, `memory_planned_bytes{component}`,
`process_resident_memory_bytes`, `worker_resident_memory_bytes{model}` (interpreter
launchers included) and `tree_resident_memory_bytes`; alert when the last exceeds the
first.

## Does the budget hold?

`tests/memory_budget` runs one workload (400 MB written across two column families,
read back at random and scanned) in two child processes. Peak resident memory
(`memory_budget_enforcement.txt`):

| | peak resident memory | memtable peak |
|---|---|---|
| historical defaults | 346 MB | not limited |
| `CAREGRAPH_MEMORY_MB=256` | **176 MB** | 36 MB against a 38 MB limit |

The budgeted run stays inside its budget and uses about half the memory of the
unbudgeted one for the same work; the test fails if either stops being true.

## Where the memory goes in a running server

`memory_server_matrix.txt`: the real server on a copy of the real database, working
set (resident) of the whole process tree, idle after start, and after 15 s at 1,000
reads/s plus 20 s at 8 writes/s:

| configuration | idle | after load |
|---|---|---|
| both models, no budget | 733 MB | 968 MB (server 226 MB, workers 404 + 330 MB) |
| both models, `CAREGRAPH_MEMORY_MB=1300` | 733 MB | 945 MB (server 205 MB) |
| **one model** (`CAREGRAPH_GAT_MODEL=none`), budget 700 | **400 MB** | **600 MB** (server 193 MB, worker 404 MB) |

- **The Python workers are most of it, and a worker's size is fixed by what it
  imports.** A fresh worker is about 330 MB resident: about 206 MB for `torch` and
  about 120 MB for `torch_geometric`, measured by importing each in turn. Loading the
  model adds nothing measurable. Reducing that means replacing PyG's convolution
  layers, which would put the exactness suites and the attribution path at risk; it
  has not been attempted. (Windows also reports about 820 MB of committed address space
  per worker; that is reservation, not resident memory.)
- **The cheapest large saving is not loading a model you do not serve**:
  `CAREGRAPH_GAT_MODEL=none` (or `CAREGRAPH_GRAPHSAGE_MODEL=none`) removed about
  330 MB idle and 370 MB under load, 38%. Fewer workers per model
  (`CAREGRAPH_WORKERS`) does the same per worker.
- **On this graph the budget barely changes the Rust process** (226 MB to 205 MB),
  because a 40 MB database never fills a 230 MB cache. The budget matters when the
  data is bigger than the cache, as in the test above. Write latency was the same in
  all three configurations (p50 21.6, 21.7 and 21.8 ms at 8 writes/s); the first
  baseline run on the same code measured 73.8 ms, which I attribute to machine state
  at the time and did not isolate.
- The load generator (`caregraph-bench-load`) is not a factor: 8 MB resident while
  driving 500 requests/s.

## Building and testing

On Windows the memory spike in this project was never the Rust compiler or the
server: it was the **MSVC linker**. With full debug information `link.exe` used about
4 GB per test or tool binary, cargo ran as many in parallel as it had jobs, and a
rebuild of every test target grew system memory by about 6 GB, to within a few
hundred MB of the machine's 16 GB (`memory_build.txt`; this is what made a build plus
a test run exhaust memory earlier). Two settings fixed it:

1. `[profile.dev] debug = "line-tables-only"` keeps file and line information for
   backtraces and panics but drops variable-level debug info: per-link memory
   4.0 GB to 2.5 GB.
2. `[profile.dev.package.librocksdb-sys] opt-level = 2, debug = false` compiles the
   RocksDB C++ (the bulk of what is linked) optimised and without debug info: per-link
   memory 2.5 GB to **0.9 GB**.

| | per-link memory | system memory growth during a rebuild of all test targets | wall time |
|---|---|---|---|
| before | 4.0 GB | 5.9-6.5 GB (peak 15.9 of 16 GB) | 100 s incremental |
| after both | **0.9 GB** | **0.8 GB** clean, **1.8 GB** incremental | 308 s clean, **17 s** incremental |

`.cargo/config.toml` also sets `jobs = 4` and `RUST_TEST_THREADS = 4` (an explicit
`RUST_TEST_THREADS` still wins). Raise them with more RAM: fewer parallel tests is a
real trade-off, the integration suite now takes about 10 s where it took 5.5 s with a
test thread per CPU, and I did not measure how much of the memory saving comes from
that setting as against the linker changes. When you need to step
through code in a debugger, set `debug = true` in `[profile.dev]` locally; it costs
memory again. Release builds are unchanged (fat LTO, one codegen unit): they are
slow but were not the problem, and changing them would change what was benchmarked.

For the heaviest suites, run them one at a time: the embedding and fault-injection
suites start Python workers (about 330 MB each).

## Containers

`infrastructure/docker-compose/dev-stack.yml` now starts **only CareGraph** by default;
Prometheus and Grafana are the `observability` profile and Neo4j and TerminusDB the
`baselines` profile, since a development session rarely needs several gigabytes of
other people's databases (`depends_on` them is gone too: CareGraph does not use them).
Each service has a memory ceiling you set in `.env` (`CAREGRAPH_CONTAINER_MEM`,
`NEO4J_MEM_LIMIT`, ...). Keep `CAREGRAPH_MEMORY_MB` below `CAREGRAPH_CONTAINER_MEM`.

## Not covered

- A hard operating-system limit on the server (a Windows job object or a cgroup is
  the deployment's job; in Kubernetes set the container limit above the budget).
- Shrinking a Python worker below its imports (see above).
- Peak memory of release builds (fat LTO); not measured.
