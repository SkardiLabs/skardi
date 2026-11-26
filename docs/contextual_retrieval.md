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
| `openai` | any `/chat/completions` | `CONTEXTUALIZE_OPENAI_BASE_URL` (required: `https://api.openai.com/v1`, or DeepSeek, vLLM, LM Studio, a llama.cpp server); `CONTEXTUALIZE_OPENAI_API_KEY` is sent to that endpoint; `OPENAI_API_KEY` is sent only when the endpoint is `https://api.openai.com` |

Ollama is called through its native API rather than its OpenAI-compatible one
because only the native API accepts `num_ctx`; Ollama's default context is a
few thousand tokens and it truncates longer prompts without an error, which
would cut most of the document out of every call.

**Cost and speed.** The document travels with every chunk, so:

- A document longer than `CONTEXTUALIZE_MAX_DOCUMENT_BYTES` (default 32,000
  UTF-8 bytes, roughly 8k tokens of English and also of Chinese) is cut into
  fixed segments of that size, each overlapping the previous by an eighth,
  and every chunk is sent with the segment it starts in. The cost of one call
  is therefore bounded however long the document is. The budget is in bytes
  rather than characters because a Chinese character is about one token
  where an English word of five letters is too: 32k Chinese *characters*
  would be 20–30k tokens and overflow a local model's context.
- Within one call the UDF handles one segment at a time (a short document is
  one segment): the first chunk is sent alone, then the remaining chunks of
  that segment go out `CONTEXTUALIZE_CONCURRENCY` at a time — default 4 for
  `anthropic` / `openai`, 1 for `ollama`. Chunks of one segment share
  byte-identical text, laid out document-first, so the first call can
  populate the provider's prompt cache and the rest read from it:
  `anthropic` marks the document block `cache_control: ephemeral` (a cache
  write is billed at 1.25× the input price and each read at 0.1×), hosted
  OpenAI caches long shared prefixes automatically. A segment shorter than
  the model's minimum cacheable prompt (1,024–4,096 tokens depending on the
  Anthropic model; 1,024 on OpenAI) is not cached at all and every chunk
  pays the full input price for it — this is the usual case for short
  documents, where the document is cheap anyway.
- A local Ollama queues requests, and concurrent calls evict each other's
  cached prefix: measured with `qwen2.5:3b` on an M5 and ~8k-token
  documents, 4 at a time averaged ~7.5 s per chunk, one at a time ~4.5 s.
- Output is capped at 200 tokens; the prompt asks for one or two sentences.

**Data leaves the machine** for `anthropic` and for `openai` (unless the base
URL points at a local server): every chunk *and its document segment* is
sent, once per chunk. `ollama` sends nothing off the host. `openai` has no
default endpoint: without `CONTEXTUALIZE_OPENAI_BASE_URL` every call fails,
so documents never go to a hosted API by accident. An `OPENAI_API_KEY` in
the environment is never sent anywhere but `api.openai.com`; a key for
another server goes in `CONTEXTUALIZE_OPENAI_API_KEY`.

**Every provider whose settings are present is callable.** The server
registers all three providers, so anyone who can send SQL to it can call any
provider its environment has a key for. Start the server with only the keys
the workspace uses.

**Runtime.** The UDF waits on HTTP calls, which needs Tokio's
multi-threaded runtime (skardi-server's); on a single-threaded runtime it
returns an error.

**Failure is loud.** Retries cover 429 and 5xx (honouring `Retry-After`) and
connection errors, up to four attempts. After that, or on any other HTTP
error, the statement fails — an ingest never writes a chunk without its
context — and the call stops at the first failure: requests in flight are
dropped and no further chunk is sent.

**Retries are paid twice.** Contexts are not stored anywhere until the
statement commits. If the statement fails, or the HTTP client that sent the
ingest request gives up waiting, every context already generated for that
document is lost — a client-side timeout does not even stop the server, which
keeps calling the model to the end — and ingesting the document again pays
for all of them again. Give contextualising ingests a generous client
timeout (minutes per document, not seconds).

**Leave room for the context in the embedding model.** The stored and
embedded text is `context + blank line + chunk`, so it is up to 200 tokens
longer than the chunk. Embedding models that reject over-long input (the
512-token BERT family, e.g. `bge-small-en-v1.5`, through `candle`) then fail
the insert *after* the contexts were paid for. Size chunks so that chunk +
200 tokens fits: with a 512-token embedder, about 800 characters of English
prose and less for code or CJK text.

**Things that do not work with every model.**

- OpenAI reasoning models (the `o`-series and reasoning `gpt-5` models) reject
  `max_tokens` (they take `max_completion_tokens`) and a `temperature` other
  than the default, both of which this UDF sends; use a non-reasoning chat
  model, or a server that ignores them.
- `<think>` blocks are stripped from the answer, but reasoning models still
  spend output tokens and time on them. A model that is still thinking when
  it hits the 200-token cap leaves no context; that fails the statement
  rather than storing the chunk without one.

**Determinism.** Requests use temperature 0, but model output can still vary
between runs and model versions, so re-ingesting the same document can store
slightly different text under the same ids.

## `rerank`

```sql
WITH candidates AS ( /* hybrid search, top 50 */ )
SELECT id, source, content,
       rerank('/models/bge-reranker-base', {query}, content) AS rerank_score
FROM candidates
ORDER BY rerank_score DESC NULLS LAST, id ASC
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
position table is smaller), the longer side first: a normal query and
passage lose only the end of the passage, and a query that alone fills the
budget is trimmed too rather than failing. A directory with several
`.safetensors` files and no `model.safetensors` (a sharded checkpoint) is
refused.
Models load on first use and stay cached per directory.

**Cost.** One forward pass per (query, passage) pair, on CPU, batched 16 at a
time. Latency grows linearly with the number of candidates, so re-score a few
dozen, not the whole table: measured on an Apple M5 CPU, 50 candidates of
~250 tokens took ~34 s with `bge-reranker-base` and ~6 s with
`ms-marco-MiniLM-L-6-v2`. Nothing leaves the machine.

`NULL` query or passage → `NULL` score.

## Measured effect

See the `auto-context` skill in
[skardi-skills](https://github.com/SkardiLabs/skardi-skills), which renders
both into its pipelines behind opt-in flags and records the recall and latency
measured on a small corpus.
