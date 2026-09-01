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

# The `documents` feature does not link PDFium into the binary: liteparse's
# pdfium-sys downloads libpdfium.so at build time (to ~/.cache/pdfium-rs/)
# and dlopens it at runtime, so the runtime image must carry the .so.
# Stage it here; the directory stays empty for builds without `documents`.
RUN mkdir -p /pdfium-lib && \
    if [ -d /root/.cache/pdfium-rs ]; then \
      find /root/.cache/pdfium-rs -name 'libpdfium.so' -exec cp {} /pdfium-lib/ \; ; \
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
# pdfium-sys searches for libpdfium.so next to the executable at runtime;
# empty (no-op) for builds without the `documents` feature.
COPY --from=builder /pdfium-lib/ /usr/local/bin/

EXPOSE 8080

ENTRYPOINT ["skardi-server"]
