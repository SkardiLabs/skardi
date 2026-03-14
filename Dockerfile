# Build stage
FROM rust:1.94.0-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    cmake \
    protobuf-compiler \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cargo build --release -p skardi-server

# Runtime stage - debian-slim includes all required runtime dependencies
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    libssl3t64 \
    zlib1g \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/skardi-server /usr/local/bin/skardi-server

EXPOSE 8080

ENTRYPOINT ["skardi-server"]
