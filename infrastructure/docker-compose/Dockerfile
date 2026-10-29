# CareGraph service images.
#
# Targets:
#   runtime-reader  no Python; serves reads / replicates (small, smallest attack surface)
#   runtime-writer  (default, last stage) Python 3.11 + CPU torch + torch_geometric +
#                   ml/embedding_server.py + ml/deployed, so the container can actually
#                   compute embeddings. Build with: docker build --target runtime-reader .
#
# RocksDB is compiled from source by the `rocksdb` crate's build script, which needs
# clang/libclang (bindgen) and cmake. build.rs compiles proto/caregraph.proto and the
# native encryption shim on every build (dummy pre-warm included), so protoc, proto/ and
# native/ must exist before the first `cargo build`.
#
# Dependencies are built in their own layer so iterating on src/ does not rebuild RocksDB.
# Cargo resolves EVERY declared [[bin]] path before building any of them, so the
# dependency layer stubs every path listed in Cargo.toml. The stub list is generated from
# Cargo.toml itself, so adding a binary can never silently break this layer again.

# ---- builder ---------------------------------------------------------------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev cmake llvm-dev pkg-config libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml rust-toolchain.toml build.rs ./
COPY proto ./proto
COPY native ./native
RUN mkdir -p src \
    && echo '' > src/lib.rs \
    && grep -E '^path = "' Cargo.toml | sed -E 's/^path = "(.*)"/\1/' | sort -u | while read -r p; do \
         mkdir -p "$(dirname "$p")"; echo 'fn main() {}' > "$p"; \
       done \
    && cargo build --release --bin caregraph \
    && rm -rf src

COPY src ./src
COPY tests ./tests
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --bin caregraph

# ---- shared runtime base ---------------------------------------------------
FROM debian:bookworm-slim AS runtime-base

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 caregraph

COPY --from=builder /build/target/release/caregraph /usr/local/bin/caregraph
COPY --from=builder /build/target/release/caregraph-admin /usr/local/bin/caregraph-admin
RUN mkdir -p /var/lib/caregraph && chown -R caregraph:caregraph /var/lib/caregraph
WORKDIR /var/lib/caregraph
ENV CAREGRAPH_DB_PATH=/var/lib/caregraph/db
EXPOSE 50051 9100
ENTRYPOINT ["/usr/local/bin/caregraph"]

# ---- reader: no Python ------------------------------------------------------
FROM runtime-base AS runtime-reader
USER caregraph

# ---- writer: Python + torch + deployed models -------------------------------
FROM runtime-base AS runtime-writer

RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu \
    && /opt/venv/bin/pip install --no-cache-dir torch_geometric numpy

COPY ml/embedding_server.py /opt/caregraph/ml/embedding_server.py
COPY ml/deployed /opt/caregraph/ml/deployed

ENV CAREGRAPH_ML_DIR=/opt/caregraph/ml \
    CAREGRAPH_PYTHON=/opt/venv/bin/python
USER caregraph
