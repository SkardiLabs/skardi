# Remote Embedding Demo (OpenAI)

Semantic search over a small knowledge base using OpenAI's
`text-embedding-3-small` model via Skardi's `remote_embed()` UDF.

## Prerequisites

1. **OpenAI API key** — set the environment variable:
   ```bash
   export OPENAI_API_KEY=sk-...
   ```
2. **Python dependencies** (for the one-time setup script):
   ```bash
   pip install openai lancedb pyarrow
   ```
3. **Build Skardi with the `remote-embed` feature**:
   ```bash
   cargo build --bin skardi-server --features remote-embed
   ```

## Setup

Run from the **project root**:

```bash
python demo/embeddings/remote/setup_remote.py
```

This will:
1. Load `data/docs.csv` (15 short knowledge-base articles)
2. Embed every document with OpenAI `text-embedding-3-small` (1536-dim)
3. Write a Lance dataset to `data/doc_embeddings_openai.lance`

## Start the server

```bash
cargo run --bin skardi-server --features remote-embed -- \
  --ctx demo/embeddings/remote/ctx.yaml \
  --pipeline demo/embeddings/remote/pipelines/ \
  --port 8080
```

## Query

```bash
curl -s "http://localhost:8080/semantic-search-remote/execute" \
  -H 'Content-Type: application/json' \
  -d '{"query": "how does semantic search work?"}' | jq .
```

The pipeline runs:

```sql
SELECT id, title, content, _distance
FROM lance_knn(
  'doc_embeddings_openai',
  'embedding',
  remote_embed('openai', 'text-embedding-3-small', {query}),
  10
)
ORDER BY _distance
LIMIT 10
```

`remote_embed()` calls the OpenAI API to embed the user query at request
time; `lance_knn()` finds the nearest documents in the pre-built Lance index.

## Switching providers

The `remote_embed()` UDF supports four providers out of the box. To use a
different one, change the provider and model in the pipeline SQL and re-run
the setup script with the corresponding embedding API:

| Provider | Example model | Env var |
|----------|---------------|---------|
| `openai` | `text-embedding-3-small` | `OPENAI_API_KEY` |
| `gemini` | `text-embedding-004` | `GEMINI_API_KEY` |
| `voyage` | `voyage-3` | `VOYAGE_API_KEY` |
| `mistral` | `mistral-embed` | `MISTRAL_API_KEY` |
