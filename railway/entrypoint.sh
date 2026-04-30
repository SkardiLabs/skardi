#!/bin/sh
# Railway entrypoint for the Skardi template.
#
# - Ensures the SQLite database exists on the persistent volume (/data).
#   First boot copies the baked-in seed; subsequent boots keep the user's data.
# - Starts skardi-server on the port Railway provides via $PORT.
set -eu

DATA_DIR="${DATA_DIR:-/data}"
DB_PATH="${DATA_DIR}/backend.db"
PORT="${PORT:-8080}"

mkdir -p "$DATA_DIR"

if [ ! -f "$DB_PATH" ]; then
    echo "Seeding $DB_PATH from /app/seed/backend.db"
    cp /app/seed/backend.db "$DB_PATH"
fi

exec skardi-server \
    --ctx /app/ctx.yaml \
    --pipeline /app/pipelines \
    --port "$PORT"
