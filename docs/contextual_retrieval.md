# Contextual retrieval — `contextualize()` and `rerank()`

Two scalar UDFs that implement the two halves of Anthropic's
[Contextual Retrieval](https://www.anthropic.com/research/contextual-retrieval)
inside an ordinary ingest / search pipeline:

| UDF | When it runs | What it does | Build feature |
|---|---|---|---|
| `contextualize(provider, model, document, chunk) -> Utf8` | ingest | A chat model reads the chunk's whole document and writes a short context that situates the chunk. Prepend it to the chunk before embedding and full-text indexing. | `contextualize` (in `rag`) |
| `rerank(model_dir, query, passage) -> Float32` | search | A local cross-encoder scores how well `passage` answers `query`. Use it to re-order a few dozen hybrid-search candidates. | `candle` (in `rag`) |

Both are optional building blocks: no existing pipeline calls them, and a
server built with the features but given pipelines that do not use them
behaves exactly as before.

## `contextualize`

```sql
-- inside an ingest pipeline: chunk, contextualize, then embed the combined text
INSERT INTO kb.main.documents (id, source, chunk_idx, content, embedding)
SELECT id, source, chunk_idx, content, vec_to_binary(candle('/models/bge-small-en-v1.5', content))
FROM (
  SELECT CAST({doc_id} AS BIGINT) * 1000 + chunk_idx AS id,
         {source} AS source,
         chunk_idx,
         concat(contextualize('ollama', 'qwen2.5:3b', {content}, chunk_text),
                chr(10), chr(10), chunk_text) AS content
  FROM (
    SELECT UNNEST(generate_series(CAST(0 AS BIGINT), CAST(array_length(chunks) AS BIGINT) - 1)) AS chunk_idx,
           UNNEST(chunks) AS chunk_text
    FROM (SELECT chunk('markdown', {content}, 1200, 150) AS chunks) c
  ) r
) AS t
```

Compute it in a subquery, as above, and read the column by name afterwards:
the UDF is `Volatile`, so writing the call twice runs the model twice.

**Arguments.** `provider` and `model` must be string literals; `document` and
`chunk` are text columns or literals. Returns the context (trimmed, with any
leading `<think>…</think>` block removed), `NULL` for a `NULL` chunk, and `''`
for an empty chunk without calling the model. A `NULL` document falls back to
the chunk itself.

**Providers.**

| `provider` | Endpoint | Credentials / settings |
|---|---|---|
| `ollama` | Ollama's native `/api/chat` (local) | `CONTEXTUALIZE_OLLAMA_BASE_URL` (default `http://localhost:11434`), `CONTEXTUALIZE_OLLAMA_NUM_CTX` (default 16384) |
| `anthropic` | Messages API | `ANTHROPIC_API_KEY`; `CONTEXTUALIZE_ANTHROPIC_URL` to override the endpoint |
| `openai` | any `/chat/completions` | `OPENAI_API_KEY` (sent only when set); `CONTEXTUALIZE_OPENAI_BASE_URL` for DeepSeek, vLLM, LM Studio, llama.cpp server |

Ollama is called through its native API rather than its OpenAI-compatible one
because only the native API accepts `num_ctx`; Ollama's default context is a
few thousand tokens and it truncates longer prompts without an error, which
would cut most of the document out of every call.

**Cost and speed.** The document travels with every chunk, so:

- Within one call the UDF handles one document at a time: the first chunk is
  sent alone, then the remaining chunks go out `CONTEXTUALIZE_CONCURRENCY`
  (default 4) at a time. The first call populates the provider's prompt cache
  — `anthropic` marks the document block `cache_control: ephemeral`, hosted
  OpenAI caches long shared prefixes automatically, and the prompt is laid out
  document-first for both — and the rest read from it.
- A document longer than `CONTEXTUALIZE_MAX_DOCUMENT_CHARS` (default 32,000)
  is cut to a window of that size centred on the chunk, so the cost of one call
  is bounded however long the document is.
- Output is capped at 200 tokens; the prompt asks for one or two sentences.

**Data leaves the machine** for `anthropic` and for `openai` (unless the base
URL points at a local server): every chunk *and its whole document* is sent,
once per chunk. `ollama` sends nothing off the host.

**Failure is loud.** Retries cover 429 and 5xx (honouring `Retry-After`) and
connection errors, up to four attempts. After that, or on any other HTTP
error, the statement fails — an ingest never writes a chunk without its
context.

**Determinism.** Requests use temperature 0, but model output can still vary
between runs and model versions, so re-ingesting the same document can store
slightly different text under the same ids.

## `rerank`

```sql
WITH candidates AS ( /* hybrid search, top 50 */ )
SELECT id, source, content,
       rerank('/models/bge-reranker-base', {query}, content) AS rerank_score
FROM candidates
ORDER BY rerank_score DESC, id ASC
LIMIT {limit}
```

**Models.** Chosen by `config.json`'s `architectures`:

| `architectures` | Checkpoints |
|---|---|
| `XLMRobertaForSequenceClassification` | `BAAI/bge-reranker-base`, `bge-reranker-large`, `bge-reranker-v2-m3` (multilingual) |
| `BertForSequenceClassification` | `cross-encoder/ms-marco-MiniLM-L-6-v2`, `ms-marco-MiniLM-L-12-v2` |

The directory needs one `.safetensors` weights file, `config.json` and
`tokenizer.json`. Only single-logit heads (`num_labels = 1`) are accepted; the
score is that raw logit, so higher is more relevant and values are comparable
only within one model. Pairs are cut to 512 tokens (fewer if the model's
position table is smaller) on the passage side; the query is never cut.
Models load on first use and stay cached per directory.

**Cost.** One forward pass per (query, passage) pair, on CPU, batched 16 at a
time. Latency grows linearly with the number of candidates, so re-score a few
dozen, not the whole table. Nothing leaves the machine.

`NULL` query or passage → `NULL` score.

## Measured effect

See the `auto-context` skill in
[skardi-skills](https://github.com/SkardiLabs/skardi-skills), which renders
both into its pipelines behind opt-in flags and records the recall and latency
measured on a small corpus.
