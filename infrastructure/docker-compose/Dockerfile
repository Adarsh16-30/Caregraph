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
# Cargo.toml itself, so adding a binary can never silently break this layer again (and a
# Windows checkout with CRLF line endings is handled: carriage returns are stripped).

# ---- builder ---------------------------------------------------------------
FROM rust:1-bookworm AS builder

# Parallel compile jobs. The default (all CPUs) can exhaust a small build machine: a full
# release build of RocksDB and the Rust dependencies at 16 jobs crashed a Docker Desktop VM on
# a 16 GB laptop. Raise it on a big builder: docker build --build-arg CARGO_BUILD_JOBS=16 ...
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev cmake llvm-dev pkg-config libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml build.rs ./
COPY proto ./proto
COPY native ./native
RUN mkdir -p src \
    && echo '' > src/lib.rs \
    && tr -d '\015' < Cargo.toml | grep -E '^path = "' | sed -E 's/^path = "(.*)"/\1/' | sort -u | while read -r p; do \
         mkdir -p "$(dirname "$p")"; echo 'fn main() {}' > "$p"; \
       done \
    && cargo build --locked --release --bin caregraph --bin caregraph-admin \
    && rm -rf src

COPY src ./src
COPY tests ./tests
RUN touch src/main.rs src/lib.rs \
    && cargo build --locked --release --bin caregraph --bin caregraph-admin

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

# Three things here are load-bearing, each learned from a failed build:
#  * Bookworm's pip (23.0.1) rejects the PyTorch mirror's typing_extensions 4.16 metadata
#    (the name is spelled with an underscore), falls back to building the source package, and
#    that needs flit_core, which the PyTorch-only index does not have. Upgrade pip first.
#  * PyPI is added as a fallback index so such dependencies resolve there.
#  * torch is pinned to the version the embedding models were trained and measured with,
#    as "+cpu", a local version that only the PyTorch index has. With PyPI as a second index
#    this stops pip from ever choosing PyPI's much larger CUDA build. The import at the end
#    fails the build if a CUDA build gets in anyway.
ARG TORCH_VERSION=2.15.0
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir "torch==${TORCH_VERSION}+cpu" \
         --index-url https://download.pytorch.org/whl/cpu \
         --extra-index-url https://pypi.org/simple \
    && /opt/venv/bin/pip install --no-cache-dir torch_geometric numpy \
    && /opt/venv/bin/python -c "import torch, torch_geometric; assert torch.version.cuda is None, 'CUDA build of torch'"

COPY ml/embedding_server.py /opt/caregraph/ml/embedding_server.py
COPY ml/deployed /opt/caregraph/ml/deployed

ENV CAREGRAPH_ML_DIR=/opt/caregraph/ml \
    CAREGRAPH_PYTHON=/opt/venv/bin/python
USER caregraph
