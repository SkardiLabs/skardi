#!/usr/bin/env python3
"""Seed the LLM Wiki demo database with sample pages from data/test_corpus/.

Splits each corpus markdown file by paragraph, computes a real
bge-small-en-v1.5 embedding for every chunk, and INSERTs into wiki_pages so
`skardi grep` works out of the box. The AFTER INSERT trigger fans each row
to wiki_pages_fts and wiki_pages_vec, mirroring what `skardi write` does.

Run AFTER `setup.py` (which drops and recreates the schema). Re-running on a
seeded DB will fail on the UNIQUE(slug) constraint — re-run setup.py first.

Usage:
    pip install transformers torch sqlite-vec
    python demo/llm_wiki/setup.py
    python demo/llm_wiki/seed.py
"""

import os
import re
import sqlite3
from pathlib import Path

import numpy as np
import sqlite_vec
import torch
from transformers import AutoModel, AutoTokenizer

DB_PATH = "demo/llm_wiki/wiki.db"
MODEL_PATH = "models/generated/bge-small-en-v1.5"

SOURCES = [
    {
        "path": "data/test_corpus/alice_sample.md",
        "page_type": "alice",
        "book_title": "Alice's Adventures in Wonderland",
    },
    {
        "path": "data/test_corpus/jane_eyre_sample.md",
        "page_type": "jane-eyre",
        "book_title": "Jane Eyre",
    },
    {
        "path": "data/test_corpus/art_of_war_sample.md",
        "page_type": "art-of-war",
        "book_title": "The Art of War",
    },
]


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-") or "intro"


def parse_corpus(path: str) -> list[tuple[str, str]]:
    """Return [(chapter_heading, paragraph_text), ...] for one markdown file.

    Splits paragraphs on blank lines. Skips the top-level title and any
    `Source:` attribution line. Tracks the most recent `## ` heading as the
    chapter for each following paragraph.
    """
    chunks: list[tuple[str, str]] = []
    chapter = "Intro"
    para: list[str] = []
    for line in Path(path).read_text().split("\n"):
        if line.startswith("# "):
            continue
        if line.startswith("## "):
            if para:
                chunks.append((chapter, "\n".join(para).strip()))
                para = []
            chapter = line[3:].strip()
            continue
        if line.startswith("Source:"):
            continue
        if line.strip() == "":
            if para:
                chunks.append((chapter, "\n".join(para).strip()))
                para = []
            continue
        para.append(line)
    if para:
        chunks.append((chapter, "\n".join(para).strip()))
    return chunks


def build_pages() -> list[dict]:
    pages: list[dict] = []
    for src in SOURCES:
        chunks = parse_corpus(src["path"])
        per_chapter_idx: dict[str, int] = {}
        for chapter, content in chunks:
            chapter_slug = slugify(chapter)
            per_chapter_idx[chapter_slug] = per_chapter_idx.get(chapter_slug, 0) + 1
            n = per_chapter_idx[chapter_slug]
            slug = f"{src['page_type']}/{chapter_slug}/p{n:03d}"
            title = f"{src['book_title']} — {chapter} (¶{n})"
            pages.append(
                {
                    "slug": slug,
                    "title": title,
                    "page_type": src["page_type"],
                    "content": content,
                }
            )
    return pages


def embed(texts: list[str], tokenizer, model) -> np.ndarray:
    """CLS-pool + L2-normalize, matching the bge-small-en-v1.5 model card."""
    inputs = tokenizer(
        texts, padding=True, truncation=True, max_length=512, return_tensors="pt"
    )
    with torch.no_grad():
        out = model(**inputs)
        cls = out.last_hidden_state[:, 0]
        cls = torch.nn.functional.normalize(cls, p=2, dim=1)
    return cls.cpu().numpy().astype(np.float32)


def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(
            f"{DB_PATH} not found — run `python demo/llm_wiki/setup.py` first."
        )
    if not os.path.isdir(MODEL_PATH):
        raise SystemExit(
            f"{MODEL_PATH} not found — see README step 3 (Download the embedding model)."
        )

    pages = build_pages()
    print(f"Parsed {len(pages)} pages from {len(SOURCES)} corpus files.")

    print(f"Loading embedding model from {MODEL_PATH}...")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModel.from_pretrained(MODEL_PATH)
    model.eval()

    db = sqlite3.connect(DB_PATH)
    db.enable_load_extension(True)
    sqlite_vec.load(db)
    db.enable_load_extension(False)

    batch_size = 16
    for i in range(0, len(pages), batch_size):
        batch = pages[i : i + batch_size]
        embs = embed([p["content"] for p in batch], tokenizer, model)
        db.executemany(
            "INSERT INTO wiki_pages (slug, title, page_type, content, embedding) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (p["slug"], p["title"], p["page_type"], p["content"], emb.tobytes())
                for p, emb in zip(batch, embs)
            ],
        )
        print(f"  embedded {min(i + batch_size, len(pages))}/{len(pages)}")

    db.execute(
        "INSERT INTO wiki_log (event_type, slug, message) VALUES (?, ?, ?)",
        ("ingest", "", f"Seeded {len(pages)} pages from data/test_corpus/."),
    )
    db.commit()
    db.close()

    print(f"Seeded {len(pages)} pages into {DB_PATH}.")
    print("Try: skardi ls | head; skardi grep 'rabbit hole'; skardi grep 'art of war'")


if __name__ == "__main__":
    main()
