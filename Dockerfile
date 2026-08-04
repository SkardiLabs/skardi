# Build stage
# The Rust version is pinned by rust-toolchain.toml (copied in with the source),
# which rustup honors even if this tag drifts. Keep the tag matching the pin so
# the build uses the preinstalled toolchain instead of downloading one.
FROM rust:1.96.1-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    cmake \
    protobuf-compiler \
    g++ \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

ARG FEATURES=""
RUN if [ -n "$FEATURES" ]; then \
      cargo build --release -p skardi-server --features "$FEATURES"; \
    else \
      cargo build --release -p skardi-server; \
    fi

# Runtime stage - debian-slim includes all required runtime dependencies
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    libssl3t64 \
    libsqlite3-0 \
    zlib1g \
    libgomp1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/skardi-server /usr/local/bin/skardi-server

EXPOSE 8080

ENTRYPOINT ["skardi-server"]
