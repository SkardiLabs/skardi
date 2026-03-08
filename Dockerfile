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

# Runtime stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/skardi-server /usr/local/bin/skardi-server

EXPOSE 8080

ENTRYPOINT ["skardi-server"]
