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

# Runtime stage - distroless/cc includes libstdc++ and ca-certificates
FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/skardi-server /usr/local/bin/skardi-server

EXPOSE 8080

ENTRYPOINT ["skardi-server"]
