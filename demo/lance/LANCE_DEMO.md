# Lance Vector Search Demo

This demo showcases Skardi's integration with Lance for high-performance vector similarity search. It demonstrates:
- Native ANN (Approximate Nearest Neighbor) search using Lance's Scanner.nearest() API
- Explicit KNN search via the `lance_knn` table function
- Seamless SQL integration for vector search queries

## Datasets

The demo includes two pre-built Lance datasets under `data/`:

### vec_data.lance
General-purpose vector embeddings for similarity search:
- **id**: int64 - Unique identifier
- **vector**: fixed_size_list\<float\>[128] - 128-dimensional embedding vector
- **item_id**: int64 - Reference to associated item
- **revenue**: double - Revenue associated with the item

### movie_embeddings.lance
Movie embeddings for recommendation pipelines:
- **movie_id**: int64 - Movie identifier
- **embedding**: fixed_size_list\<float\>[128] - 128-dimensional movie embedding

## Demo Components

| File | Description |
|------|-------------|
| `ctx_lance.yaml` | Context file registering `vec_data.lance` |
| `pipeline_lance.yaml` | KNN similarity search pipeline |
| `ctx_movie_recommendation.yaml` | Context with Lance embeddings + PostgreSQL movies table |
| `pipeline_movie_recommendation.yaml` | Movie recommendation pipeline (KNN + ONNX ranking) |

## How It Works

### lance_knn Table Function

Use the `lance_knn` table function for explicit KNN search:
```sql
SELECT * FROM lance_knn(table_name, vector_column, query_vector, k, [filter])
```

Parameters:
- `table_name`: Name of the Lance table (string)
- `vector_column`: Name of the embedding column (string)
- `query_vector`: Query vector as literal array or scalar subquery
- `k`: Number of nearest neighbors (integer)
- `filter`: Optional Lance filter predicate (string)

### Query Execution

The table function directly calls Lance's KNN API:
```
lance_knn(...) -> LanceKnnExec -> Lance Scanner.nearest()
```

### Performance Benefits

- **Without optimization**: O(N * D + N log N) - full scan + sort
- **With Lance KNN**: O(k log N) - index-based ANN search
- **Typical speedup**: 10x-1000x for datasets with N > 100K vectors

| Dataset Size | Without Optimization | With Lance KNN | Speedup |
|--------------|---------------------|----------------|---------|
| 10K vectors  | ~50ms              | ~5ms           | 10x     |
| 100K vectors | ~500ms             | ~8ms           | 62x     |
| 1M vectors   | ~5000ms            | ~15ms          | 333x    |
| 10M vectors  | ~50000ms           | ~25ms          | 2000x   |

*Benchmarks: 128-dim vectors, k=10, IVF-PQ index, Intel Core i9*

## Running the Demo

### Start the Server

```bash
cargo run --bin skardi-server -- \
  --ctx demo/lance/ctx_lance.yaml \
  --pipeline demo/lance/pipeline_lance.yaml \
  --port 8080
```

Expected output:
```
Starting Skardi Online Serving Pipeline Server
CLI Arguments parsed successfully
   Pipeline file: Some("demo/lance/pipeline_lance.yaml")
   Context file: Some("demo/lance/ctx_lance.yaml")
   Port: 8080
Server configuration loaded successfully
   Pipeline: lance-vector-similarity-search
   Data sources: 1
Server listening on 0.0.0.0:8080
```

### Example 1: Find Similar Items

Find the top 10 items most similar to item with id=1:

```bash
curl -X POST http://localhost:8080/lance-vector-similarity-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "reference_id": 1,
    "min_revenue": null,
    "max_revenue": null,
    "k": 10
  }'
```

Response:
```json
{
  "success": true,
  "data": [
    {
      "id": 42,
      "item_id": 1337,
      "revenue": 2500.50,
      "distance": 0.125
    },
    {
      "id": 89,
      "item_id": 2048,
      "revenue": 1800.25,
      "distance": 0.187
    }
  ],
  "rows": 10,
  "execution_time_ms": 8
}
```

### Example 2: Filtered Similarity Search

Find similar items with revenue constraints:

```bash
curl -X POST http://localhost:8080/lance-vector-similarity-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "reference_id": 1,
    "min_revenue": 1000.0,
    "max_revenue": 5000.0,
    "k": 5
  }'
```

### Example 3: Adjust Top-K Results

Get more or fewer results by changing `k`:

```bash
curl -X POST http://localhost:8080/lance-vector-similarity-search/execute \
  -H "Content-Type: application/json" \
  -d '{
    "reference_id": 1,
    "min_revenue": null,
    "max_revenue": null,
    "k": 50
  }'
```

## Pipeline Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `reference_id` | integer | Yes | ID of the reference item to find similar items for |
| `min_revenue` | double | No | Minimum revenue filter (null = no filter) |
| `max_revenue` | double | No | Maximum revenue filter (null = no filter) |
| `k` | integer | Yes | Number of nearest neighbors to return (top-k) |

## SQL Query Patterns

### Basic KNN Search

Find similar items using a subquery for the reference vector:
```sql
SELECT knn.id, knn.item_id, knn._distance as dist
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  10
) knn
WHERE knn.id != 1
```

### Using CTE for Reference Vector

```sql
WITH ref AS (
  SELECT vector FROM sift_items WHERE id = 1
)
SELECT knn.id, knn.item_id, knn._distance
FROM lance_knn('sift_items', 'vector', (SELECT vector FROM ref), 10) knn
WHERE knn.id != 1
```

### With Filter Parameter

```sql
SELECT *
FROM lance_knn(
  'sift_items',
  'vector',
  (SELECT vector FROM sift_items WHERE id = 1),
  10,
  'revenue > 1000'
)
```

### Movie Recommendation (Federated: Lance + PostgreSQL)

The `pipeline_movie_recommendation.yaml` demonstrates a more advanced use case — finding similar movies via Lance KNN, then ranking them with an ONNX model, and joining with a PostgreSQL movies table for metadata:

```sql
WITH knn_results AS (
  SELECT knn.movie_id
  FROM lance_knn(
    'movie_embeddings',
    'embedding',
    (SELECT embedding FROM movie_embeddings WHERE movie_id = (SELECT movie_id FROM movies WHERE title = {last_watched_movie})),
    10
  ) knn
)
SELECT m.title, m.genres, rr.prediction_score
FROM ranked_recommendations rr
JOIN movies m ON rr.movie_id = m.movie_id
ORDER BY rr.prediction_score DESC
LIMIT {top_n}
```

## Creating Your Own Vector Search Pipeline

### 1. Create Context Configuration

```yaml
data_sources:
  - name: "my_vectors"
    type: "lance"
    path: "data/my_vectors.lance/"
    description: "My vector embeddings"
```

### 2. Create Pipeline Configuration

```yaml
metadata:
  name: my-vector-search
  version: 1.0.0

query: |
  SELECT
    knn.id,
    knn.item_id,
    knn._distance as similarity
  FROM lance_knn(
    'my_vectors',
    'vector',
    (SELECT vector FROM my_vectors WHERE id = {query_id}),
    {k}
  ) knn
  WHERE knn.id != {query_id}
```

### 3. Run Your Pipeline

```bash
cargo run --bin skardi-server -- \
  --ctx ctx_my_vectors.yaml \
  --pipeline pipeline_my_search.yaml \
  --port 8080
```

## Monitoring Execution

Enable debug logs to see KNN execution:

```bash
RUST_LOG=debug cargo run --bin skardi-server -- \
  --ctx demo/lance/ctx_lance.yaml \
  --pipeline demo/lance/pipeline_lance.yaml \
  --port 8080
```

Look for:
```
INFO  source::lance::knn_table_function: Registering Lance table functions
INFO  LanceKnnExec: Executing KNN search
```

## Troubleshooting

### "lance_knn: table 'xxx' not found in registry"
Ensure your context file uses `type: "lance"` for the data source and the table name matches.

### "lance_knn: subquery must return exactly one column"
The query vector subquery must return a single column containing the vector.

### "lance_knn: query_vector must be literal array or scalar subquery"
The third argument must be either a literal array `[0.1, 0.2, ...]` or a scalar subquery `(SELECT vector FROM ...)`.

### "Distance values seem incorrect"
Distance metric is determined by the Lance index. Check your index configuration:
- L2 (Euclidean): Default for most cases
- Cosine: Better for normalized vectors
- Dot Product: For inner product similarity

## Additional Resources

- [Lance Documentation](https://lancedb.github.io/lance/)
- [DataFusion Integration](https://docs.rs/datafusion/)
