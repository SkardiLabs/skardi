#!/usr/bin/env bash
# Create the SQLite database and seed tables for the simple backend demo.
set -e

DB="demo/simple_backend/backend.db"

sqlite3 "$DB" <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    id    INTEGER PRIMARY KEY AUTOINCREMENT,
    name  TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    title       TEXT NOT NULL,
    done        INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob',   'bob@example.com');

INSERT OR IGNORE INTO tasks (user_id, title) VALUES
    (1, 'Buy groceries'),
    (1, 'Write report'),
    (2, 'Book flight');
SQL

echo "Database created at $DB"
