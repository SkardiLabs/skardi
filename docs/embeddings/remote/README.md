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
1. Load `demo/embeddings/data/docs.csv` (15 short knowledge-base articles)
2. Embed every document with OpenAI `text-embedding-3-small` (1536-dim)
3. Write a Lance dataset to `data/generated/doc_embeddings_openai.lance`

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
  -d '{"query": "how does semantic search work?", "k": 10}' | jq .
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "id": 9,
      "title": "Semantic Search",
      "content": "Semantic search retrieves documents based on meaning rather than keyword overlap. A query and all documents are embedded into the same vector space; the most semantically similar documents are returned via nearest-neighbour search. This handles synonyms and paraphrases that would confuse keyword search.",
      "_distance": 0.62793195
    },
    {
      "id": 15,
      "title": "OpenAI Embeddings",
      "content": "OpenAI's text-embedding-3-small model produces 1536-dimensional vectors optimised for semantic similarity and retrieval. It supports shortening via the dimensions parameter and is trained on diverse text data. The model is accessed via the /v1/embeddings REST API with an API key.",
      "_distance": 1.2529761
    },
    {
      "id": 2,
      "title": "Retrieval-Augmented Generation",
      "content": "Retrieval-Augmented Generation (RAG) combines a retrieval step with a language model. A query is embedded into a vector and used to fetch relevant documents from a vector store. Those documents are passed as context to an LLM",
      "_distance": 1.2652609
    },
    {
      "id": 4,
      "title": "BERT Embeddings",
      "content": "BERT (Bidirectional Encoder Representations from Transformers) produces contextual embeddings by reading text in both directions. Fine-tuned variants like bge-small and all-MiniLM are commonly used for semantic similarity tasks. The [CLS] token or mean-pooled hidden states are used as sentence-level embeddings.",
      "_distance": 1.2673755
    },
    {
      "id": 11,
      "title": "Approximate Nearest Neighbour Search",
      "content": "Exact nearest-neighbour search scales as O(n) per query. ANN algorithms like HNSW and IVF-PQ trade a small accuracy loss for sub-linear query times",
      "_distance": 1.3190017
    },
    {
      "id": 1,
      "title": "Vector Databases",
      "content": "Vector databases store high-dimensional numerical vectors and enable fast similarity search at scale. Unlike traditional databases that match exact values",
      "_distance": 1.3902473
    },
    {
      "id": 12,
      "title": "Remote Embeddings",
      "content": "Remote embedding APIs let you generate high-quality vector embeddings without downloading or hosting models locally. Providers like OpenAI",
      "_distance": 1.5463313
    },
    {
      "id": 7,
      "title": "DataFusion Query Engine",
      "content": "DataFusion is an in-process SQL query engine written in Rust",
      "_distance": 1.5478841
    },
    {
      "id": 3,
      "title": "Transformer Architecture",
      "content": "The Transformer architecture introduced multi-head self-attention to replace recurrent networks. Each token attends to all other tokens in the sequence",
      "_distance": 1.5975192
    },
    {
      "id": 13,
      "title": "Mean Pooling",
      "content": "Mean pooling aggregates the per-token hidden states from a transformer encoder into a single fixed-size vector. Each token embedding is averaged across the sequence length",
      "_distance": 1.5982214
    }
  ],
  "rows": 10,
  "execution_time_ms": 1733,
  "timestamp": "2026-04-08T17:16:42.575794+00:00"
}
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
