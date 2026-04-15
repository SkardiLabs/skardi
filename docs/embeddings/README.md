# Embeddings

Skardi can generate text embeddings inline in SQL via three different backends, and combine them with `lance_knn()` for end-to-end semantic search — query is embedded, nearest documents are retrieved, results stream back — all in one SQL statement, with no application code in between.

```sql
SELECT id, title, content, _distance
FROM lance_knn(
  'doc_embeddings',
  'embedding',
  candle('models/generated/bge-small-en-v1.5', {query}),  -- embed the query
  10                                                       -- top-k
)
ORDER BY _distance
LIMIT 10
```

The shape is always the same: an embedding UDF (`candle`, `gguf`, or `remote_embed`) takes the user's text, returns a `List<Float32>`, and `lance_knn` uses that vector to find the nearest pre-embedded documents in a Lance dataset.

## Choosing a Backend

| Backend | UDF | When to use it | Build feature |
|---|---|---|---|
| **[Candle](candle/)** | `candle(model_dir, text)` | Local SafeTensors models (HuggingFace BERT/RoBERTa/DistilBERT/Jina). No network, no API key, fastest cold start once cached. Best default for self-hosted setups. | `--features candle` |
| **[GGUF](gguf/)** | `gguf(model_dir, text)` | Local GGUF / llama.cpp models with quantisation (Q4/Q5/Q8/F16). Use when you want to run a larger model with a smaller memory footprint, or when the model only ships in GGUF format (e.g. `embeddinggemma`). | `--features gguf` |
| **[Remote](remote/)** | `remote_embed(provider, model, text)` | Hosted APIs: OpenAI, Gemini, Voyage, Mistral. Use when you want top-tier model quality without managing weights, GPUs, or memory. Requires an API key and pays per-token. | `--features remote-embed` |

All three backends produce `List<Float32>` and plug into `lance_knn()` the same way — switching backends is a one-line change in the pipeline SQL plus rebuilding the Lance dataset with the matching embedder.

Quick rule of thumb:

- **Local + small model + simple deps** → Candle.
- **Local + larger model or quantised** → GGUF.
- **No GPU, no model management, willing to pay per call** → Remote.

## How It Works

1. **Offline (one-time setup)** — A Python script in each backend's directory loads `docs/embeddings/data/docs.csv` (a 15-document knowledge base shared across all three backends), embeds every row with the chosen model, and writes a Lance dataset to that backend's `data/generated/` directory.
2. **At query time** — The user POSTs `{"query": "...", "k": 10}` to the pipeline endpoint. Skardi calls the embedding UDF on `{query}`, hands the resulting vector to `lance_knn`, and streams back the top-k nearest documents with their `_distance`.
3. **Model caching** — For the local backends (`candle`, `gguf`), the model is loaded into memory on the first call and reused for every subsequent query. There is no per-request model load.

The shared corpus means you can run all three demos against the same source data and compare results side by side.

## Pipeline Shape

Every backend uses the same parameter shape so pipelines are interchangeable:

| Parameter | Type | Required | Description |
|---|---|---|---|
| `query` | string | Yes | Free-text search query to embed and search with. |
| `k` | integer | Yes | Number of nearest neighbours to return. |

## Prerequisites Common to All Backends

1. **Python 3.12** for the setup scripts. Some backends (Candle) require 3.12 specifically because `onnxruntime` has no wheels for 3.13+.
2. **Build `skardi-server` with the matching feature** — see the table above. You can enable multiple features at once (`--features "candle gguf remote-embed"`) if you want all three backends in the same binary.
3. **Run setup and `skardi-server` from the project root**, so the `models/` and `docs/embeddings/<backend>/data/generated/` paths in the pipeline SQL resolve correctly.

Each backend's README has the exact `pip install` line, model download steps, and any backend-specific gotchas (Gemma licence acceptance for GGUF, API keys for Remote).

## Directory Layout

```
docs/embeddings/
├── README.md                 — this file
├── data/
│   └── docs.csv              — shared 15-document knowledge base
├── candle/
│   ├── README.md             — Candle (local SafeTensors) walkthrough
│   ├── ctx.yaml
│   ├── setup.py
│   └── pipelines/
├── gguf/
│   ├── README.md             — GGUF (local llama.cpp) walkthrough
│   ├── ctx.yaml
│   ├── setup_gguf.py
│   └── pipelines/
└── remote/
    ├── README.md             — Remote API (OpenAI/Gemini/Voyage/Mistral) walkthrough
    ├── ctx.yaml
    ├── setup_remote.py
    └── pipelines/
```

## See Also

- [docs/lance/](../lance/) — `lance_knn()` reference and indexing options for the vector store side of the pipeline.
- [docs/server.md](../server.md) — pipeline file format, parameter inference, and the HTTP endpoint contract.
- [demo/rag/](../../demo/rag/) — full RAG pipeline that uses one of these embedding backends end-to-end.
