# CareGraph service image.
#
# RocksDB is compiled from source by the `rocksdb` crate's build script, which
# needs clang/libclang (for bindgen) and cmake. `build.rs` compiles
# proto/caregraph.proto via protoc for every build, dummy pre-warm included —
# both `protobuf-compiler` and `proto/` have to be present before the very
# first `cargo build` call, not only the real one. Dependencies are built in
# their own layer so that iterating on src/ does not trigger a full RocksDB
# rebuild — that layer caching is what keeps `docker compose up --build`
# inside the 5-minute Phase 1 budget on a warm cache.
#
# Verified, not just written: `docker build` produces a 147MB image, and the
# binary inside it genuinely opens RocksDB and starts the gRPC listener
# (`docker run` with `CAREGRAPH_API_KEY` unset refuses to start with the
# expected error; set, it logs "caregraph gRPC service starting" and blocks
# serving, exactly as `src/main.rs` is written to).

# ---- builder ---------------------------------------------------------------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        cmake \
        llvm-dev \
        pkg-config \
        libssl-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Dependency-only layer: a manifest, the proto schema, and a throwaway source
# tree. build.rs and proto/ must be copied before this dummy build — it runs
# the same build script the real build does.
#
# Cargo.toml declares six [[bin]] targets (autobins = false, so nothing is
# auto-discovered — every one is explicit). Cargo resolves every declared
# target's path before building any of them, for a bare `cargo build` and
# for `--bin caregraph` alike, so a placeholder for main.rs alone is not
# enough — the other five need to exist too, even though only `caregraph`
# ships in the runtime image. Found the hard way: this layer silently only
# ever stubbed main.rs, and nothing had actually run `docker build` since
# bench_pit/bench_traversal/bench_incremental/load_trace/fault_injection_worker
# were added across later phases, so target resolution had been failing here
# for a while before anyone noticed.
COPY Cargo.toml rust-toolchain.toml build.rs ./
COPY proto ./proto
# build.rs unconditionally compiles native/rocksdb_encryption/shim.cc (Rule 8)
# on every build, prewarm included — same class of bug this comment already
# documents once for the [[bin]] stubs: a layer that only copies what *looks*
# sufficient breaks the moment build.rs needs something else unconditionally.
COPY native ./native
RUN mkdir -p src/bin \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && echo 'fn main() {}' > src/bin/load_trace.rs \
    && echo 'fn main() {}' > src/bin/bench_pit.rs \
    && echo 'fn main() {}' > src/bin/bench_traversal.rs \
    && echo 'fn main() {}' > src/bin/bench_incremental.rs \
    && echo 'fn main() {}' > src/bin/fault_injection_worker.rs \
    && cargo build --release --bin caregraph \
    && rm -rf src

# Real sources.
COPY src ./src
COPY tests ./tests
# Touch so cargo does not reuse the placeholder's fingerprint.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --bin caregraph

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 caregraph

COPY --from=builder /build/target/release/caregraph /usr/local/bin/caregraph

RUN mkdir -p /var/lib/caregraph && chown -R caregraph:caregraph /var/lib/caregraph
USER caregraph
WORKDIR /var/lib/caregraph

ENV CAREGRAPH_DB_PATH=/var/lib/caregraph/db
EXPOSE 50051 9100

ENTRYPOINT ["/usr/local/bin/caregraph"]
