# Railway template image for Skardi v0.3.0.
#
# Build stage: bake an initial SQLite database from seed.sql so the deployed
# service starts with working sample data without requiring sqlite3 at runtime.
FROM debian:trixie-slim AS seed
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /seed
COPY seed.sql .
RUN sqlite3 /seed/backend.db < seed.sql

FROM ghcr.io/skardilabs/skardi/skardi-server:0.3.0

WORKDIR /app

COPY ctx.yaml /app/ctx.yaml
COPY pipelines/ /app/pipelines/
COPY entrypoint.sh /app/entrypoint.sh
COPY --from=seed /seed/backend.db /app/seed/backend.db

# Railway injects $PORT and (optionally) a mounted volume at /data.
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/app/entrypoint.sh"]
