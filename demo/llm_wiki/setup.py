#!/usr/bin/env python3
"""Create the SQLite database for the LLM Wiki CLI demo.

Builds the schema only — pages and embeddings are written later through
`skardi query` (which uses the `candle()` UDF for inline embedding).

`vec0` requires an INTEGER rowid, so the human-readable `slug` lives on the
base `wiki_pages` table (and as an UNINDEXED FTS5 column) and the integer `id`
carries the JOIN to the vector mirror.

Schema:
    wiki_pages        canonical pages, slug UNIQUE, embedding BLOB
    wiki_pages_fts    FTS5 mirror (id/slug/title/page_type UNINDEXED, content indexed)
    wiki_pages_vec    sqlite-vec vec0 mirror, float[384] embeddings
    wiki_log          append-only activity log
    wiki_pages_ai     AFTER INSERT trigger fanning rows to both mirrors
    wiki_pages_au     AFTER UPDATE trigger refreshing both mirrors

Usage:
    pip install sqlite-vec
    python demo/llm_wiki/setup.py
"""

import os
import sqlite3

import sqlite_vec

DB_PATH = "demo/llm_wiki/wiki.db"

SCHEMA = """
CREATE TABLE wiki_pages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    slug       TEXT NOT NULL UNIQUE,
    title      TEXT NOT NULL,
    page_type  TEXT NOT NULL,
    content    TEXT NOT NULL,
    embedding  BLOB NOT NULL,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE VIRTUAL TABLE wiki_pages_fts USING fts5(
    id UNINDEXED, slug UNINDEXED, title UNINDEXED, page_type UNINDEXED,
    content
);

CREATE VIRTUAL TABLE wiki_pages_vec USING vec0(
    id        INTEGER PRIMARY KEY,
    embedding float[384]
);

CREATE TRIGGER wiki_pages_ai AFTER INSERT ON wiki_pages BEGIN
    INSERT INTO wiki_pages_fts(id, slug, title, page_type, content)
        VALUES (NEW.id, NEW.slug, NEW.title, NEW.page_type, NEW.content);
    INSERT INTO wiki_pages_vec(id, embedding)
        VALUES (NEW.id, NEW.embedding);
END;

CREATE TRIGGER wiki_pages_au AFTER UPDATE ON wiki_pages BEGIN
    DELETE FROM wiki_pages_fts WHERE id = OLD.id;
    INSERT INTO wiki_pages_fts(id, slug, title, page_type, content)
        VALUES (NEW.id, NEW.slug, NEW.title, NEW.page_type, NEW.content);
    DELETE FROM wiki_pages_vec WHERE id = OLD.id;
    INSERT INTO wiki_pages_vec(id, embedding)
        VALUES (NEW.id, NEW.embedding);
END;

CREATE TRIGGER wiki_pages_ad AFTER DELETE ON wiki_pages BEGIN
    DELETE FROM wiki_pages_fts WHERE id = OLD.id;
    DELETE FROM wiki_pages_vec WHERE id = OLD.id;
END;

CREATE TABLE wiki_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    slug        TEXT NOT NULL,
    message     TEXT NOT NULL,
    created_at  TEXT DEFAULT (datetime('now'))
);
"""


def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    db = sqlite3.connect(DB_PATH)
    db.enable_load_extension(True)
    sqlite_vec.load(db)
    db.enable_load_extension(False)

    db.executescript(SCHEMA)
    db.commit()
    db.close()

    print(f"Created {DB_PATH}")
    print(
        "  wiki_pages, wiki_pages_fts, wiki_pages_vec, wiki_log ready for ingest via skardi query"
    )


if __name__ == "__main__":
    main()
