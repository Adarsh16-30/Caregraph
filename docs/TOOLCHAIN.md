# Toolchain setup

CareGraph needs three things beyond a normal Rust project: a Rust toolchain, a
C++ toolchain (RocksDB compiles from source), and Docker for the baseline
systems that Rule 4 benchmarks run against.

## Windows

Install in this order — the RocksDB build fails without the C++ toolchain, and
the error message it produces is unhelpful.

### 1. Visual Studio C++ Build Tools

RocksDB is a C++ library. The `rocksdb` crate compiles it from source using MSVC
and generates bindings with `bindgen`, which needs `libclang`.

Download **Build Tools for Visual Studio** from
<https://visualstudio.microsoft.com/downloads/> (under "Tools for Visual
Studio"). In the installer select:

- **Desktop development with C++**
- Within that workload, confirm these are checked:
  - MSVC v143 build tools
  - Windows 11 SDK
  - **C++ Clang tools for Windows** — this provides `libclang`, which `bindgen`
    requires; it is *not* selected by default

### 2. Rust

Download and run `rustup-init.exe` from <https://rustup.rs>. Accept the default
`stable-x86_64-pc-windows-msvc` host triple.

Then open a **new** terminal so the updated `PATH` takes effect:

```bash
rustc --version
cargo --version
```

### 3. Docker Desktop

<https://www.docker.com/products/docker-desktop/>. Requires WSL2; the installer
will enable it if needed and prompt for a reboot.

```bash
docker --version
docker compose version
```

### 4. CMake

```bash
winget install Kitware.CMake
```

### 5. protoc

From Phase 6 on, `build.rs` runs `tonic_build` unconditionally for the
whole crate (not only when touching `src/api/`), and that needs a real
`protoc` binary on `PATH` — verified the hard way: a from-scratch build on
a machine with the rest of this toolchain already installed still fails
with `Could not find 'protoc'` until this step is done. There is no
Windows package manager entry as reliable as the other tools above, so
install it manually:

1. Download `protoc-<version>-win64.zip` from
   <https://github.com/protocolbuffers/protobuf/releases> (any recent
   release; this repository has been built against 29.x).
2. Unzip it somewhere permanent and add its `bin/` directory to `PATH`, or
   set `PROTOC` to the full path of `protoc.exe` directly — `tonic-build`
   (via `prost-build`) checks `PROTOC` before searching `PATH`.

```bash
protoc --version   # should print "libprotoc <version>"
```

### Verify

From the repository root:

```bash
cargo build
cargo test --lib
cargo test --test integration
```

If `bindgen` reports it cannot find `libclang.dll`, the Clang component from
step 1 is missing. Point `LIBCLANG_PATH` at it as a fallback:

```powershell
$env:LIBCLANG_PATH = "C:\Program Files\Microsoft Visual Studio\2022\BuildTools\VC\Tools\Llvm\x64\bin"
```

## Linux / macOS

```bash
# Debian/Ubuntu — protobuf-compiler is protoc, needed unconditionally from
# Phase 6 on (see the Windows section's own note on this); ci.yml installs
# the same package for the same reason.
sudo apt-get install -y clang libclang-dev cmake llvm-dev pkg-config libssl-dev protobuf-compiler

# macOS
brew install cmake llvm protobuf

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

---

## Known dependency risk: the RocksDB crate version

**PRD 2.1 specifies "rust-rocksdb 0.43+".** That version does not correspond to
the crate this project depends on, and the discrepancy needs resolving before
the version is treated as settled.

`rust-rocksdb` is the name of the *GitHub repository*
(<https://github.com/rust-rocksdb/rust-rocksdb>). The crate it publishes to
crates.io is named **`rocksdb`**, and its own version line is `0.2x` — it has
never reached 0.43. There are also third-party forks published under other
names whose version numbers track upstream RocksDB releases more closely.

`Cargo.toml` currently pins:

```toml
rocksdb = { version = "0.23", features = ["multi-threaded-cf", "lz4"] }
```

**Action required at first build:** run `cargo build` and confirm the resolved
version. If the PRD meant a specific fork rather than the canonical crate,
switch the dependency and re-check the API surface used in `src/storage/kv.rs`
— particularly `DBWithThreadMode`, `BoundColumnFamily`, and the
`iterator_cf_opt` signature, which differ between forks.

This is flagged rather than silently resolved because the choice affects the
encryption-at-rest work in Phase 7: RocksDB's native encryption support is not
exposed identically across forks, and Rule 8 depends on reaching it.

## Other version notes

| Dependency | PRD says | Pinned | Note |
|-----------|----------|--------|------|
| RocksDB crate | 0.43+ | `0.23` | See above |
| tokio | 1.x | `1` | Matches |
| tonic | 0.11+ | — | Added at Phase 6 |
| Neo4j | 5.x | `5.26-community` | GDS plugin loaded via `NEO4J_PLUGINS` |
| TerminusDB | latest | `v11.1.14` | Pinned, not `latest` — a moving baseline makes benchmark numbers untraceable (Rule 10) |
| Prometheus | 2.5x | `v2.55.1` | Matches |
| Grafana | 10.x | `10.4.14` | Matches |

## Path discrepancy in the PRD

Section 5.2 and Rule 4 refer to `bench/run_baseline.sh`; Section 10's directory
structure specifies `benchmarks/run_baseline.sh`. This project uses
**`benchmarks/`**, following the Section 10 layout, and `scripts/check_rules.sh`
checks that path. Worth correcting in the PRD so the two sections agree.
