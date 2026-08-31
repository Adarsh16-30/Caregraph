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
COPY Cargo.toml rust-toolchain.toml build.rs ./
COPY proto ./proto
RUN mkdir -p src \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release \
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
