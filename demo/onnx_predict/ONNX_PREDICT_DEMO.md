# ONNX Predict Demo

This demo shows how to use the `onnx_predict` scalar UDF to run ONNX model inference directly inside SQL queries. Skardi loads ONNX models lazily on first use and caches them in memory — no pre-registration or configuration required.

## How It Works

`onnx_predict` is a DataFusion scalar UDF with the following signature:

```sql
onnx_predict('path/to/model.onnx', input1, input2, ...) -> FLOAT
```

- **First argument**: A string literal path to an `.onnx` file (relative to the working directory)
- **Remaining arguments**: Model inputs — their types and count must match what the ONNX model expects
- **Returns**: `FLOAT` (scalar prediction per row), or `LIST(FLOAT)` when inputs are aggregated lists

### Key Behaviors

- **Lazy loading**: The ONNX runtime (ORT) is only initialized when the first model is loaded. Models are cached by file path after the first call.
- **Automatic type detection**: Input/output types and shapes are introspected from the ONNX model metadata — no manual configuration needed.
- **Integer input models**: Models that expect integer inputs (e.g., NCF-style user/item ID models) are automatically detected and processed row-by-row.
- **Float input models**: Models with float inputs (e.g., time-series, embedding models) are batched into ndarrays.
- **List mode**: When any input is a `ListArray` (e.g., from `array_agg`), the output is wrapped in `LIST(FLOAT)` to preserve row cardinality.

## Prerequisites

1. An ONNX model file (`.onnx`) accessible from the server's working directory
2. The Skardi server built with model support:
   ```bash
   cargo build --release -p skardi-server
   ```

## Example: Movie Recommendation Pipeline

This example combines Lance vector search with ONNX model inference to build a movie recommendation system. It finds similar movies via KNN, then ranks them using a Neural Collaborative Filtering (NCF) model.

### Data Sources

The context file (`ctx_movie_recommendation.yaml`) defines two data sources:

```yaml
data_sources:
  - name: "movies"
    type: "postgres"
    connection_string: "postgresql://localhost:5432/test?sslmode=disable"
    options:
      table: "movies"
      schema: "public"
      user_env: "PG_USER"
      pass_env: "PG_PASSWORD"
    description: "Movies table with movie_id, title, genres, year"

  - name: "movie_embeddings"
    type: "lance"
    path: "data/movie_embeddings.lance"
    description: "128-dimensional movie embeddings"
```

### Pipeline

The pipeline (`pipeline_movie_recommendation.yaml`) chains three steps in a single SQL query:

```sql
-- Step 1: Find the movie by title
WITH last_watched AS (
  SELECT movie_id, title
  FROM movies
  WHERE title = {last_watched_movie}
  LIMIT 1
),
-- Step 2: Find 10 similar movies via Lance KNN
knn_results AS (
  SELECT knn.movie_id
  FROM lance_knn(
    'movie_embeddings',
    'embedding',
    (SELECT embedding FROM movie_embeddings
     WHERE movie_id = (SELECT movie_id FROM last_watched)),
    10
  ) knn
  WHERE knn.movie_id != (SELECT movie_id FROM last_watched)
),
-- Step 3: Score each candidate with the NCF ONNX model
ranked_recommendations AS (
  SELECT
    kr.movie_id,
    onnx_predict('models/ncf.onnx',
      CAST({user_id} AS BIGINT),
      CAST(kr.movie_id AS BIGINT)
    ) AS prediction_score
  FROM knn_results kr
)
-- Step 4: Join with movie metadata and return top results
SELECT
  m.movie_id, m.title, m.genres, m.year, m.genres_list,
  rr.prediction_score
FROM ranked_recommendations rr
JOIN movies m ON rr.movie_id = m.movie_id
ORDER BY rr.prediction_score DESC
LIMIT {top_n}
```

### Running the Demo

```bash
# Set PostgreSQL credentials
export PG_USER="your_user"
export PG_PASSWORD="your_password"

# Start the server
cargo run --bin skardi-server -- \
  --ctx demo/onnx_predict/ctx_movie_recommendation.yaml \
  --pipeline demo/onnx_predict/pipelines/ \
  --port 8080
```

### Execute

```bash
curl -X POST http://localhost:8080/movie-recommendation-pipeline/execute \
  -H "Content-Type: application/json" \
  -d '{
    "last_watched_movie": "Toy Story (1995)",
    "user_id": 42,
    "top_n": 5
  }'
```

**Expected Response:**
```json
{
  "success": true,
  "data": [
    {
      "movie_id": 356,
      "title": "Forrest Gump (1994)",
      "genres": "Comedy|Drama|Romance|War",
      "year": 1994,
      "genres_list": ["Comedy", "Drama", "Romance", "War"],
      "prediction_score": 0.923
    }
  ],
  "rows": 5,
  "execution_time_ms": 45
}
```

### Pipeline Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `last_watched_movie` | string | Yes | Title of the movie to find recommendations for |
| `user_id` | integer | Yes | User ID for personalized scoring |
| `top_n` | integer | Yes | Number of recommendations to return |

## Using onnx_predict in Your Own Pipelines

### Scoring with Integer Inputs (ID-based models)

Models like NCF that take user/item IDs:

```sql
SELECT
  item_id,
  onnx_predict('models/ncf.onnx',
    CAST({user_id} AS BIGINT),
    CAST(item_id AS BIGINT)
  ) AS score
FROM candidates
ORDER BY score DESC
LIMIT 10
```

### Scoring with Float Inputs

Models that take numeric features:

```sql
SELECT
  id,
  onnx_predict('models/regressor.onnx',
    CAST(feature_1 AS FLOAT),
    CAST(feature_2 AS FLOAT),
    CAST(feature_3 AS FLOAT)
  ) AS prediction
FROM data_table
```

### Aggregated List Mode

When inputs come from aggregation (e.g., time-series), the output is automatically wrapped in a list:

```sql
SELECT
  group_id,
  onnx_predict('models/forecaster.onnx',
    array_agg(value ORDER BY timestamp)
  ) AS forecast
FROM time_series
GROUP BY group_id
```

## Available Models

The `models/` directory includes pre-built ONNX models:

| Model | Description | Inputs |
|-------|-------------|--------|
| `ncf.onnx` | Neural Collaborative Filtering | user_id (INT64), item_id (INT64) |
| `TinyTimeMixer.onnx` | Time-series forecasting | aggregated float sequences |

## Troubleshooting

### "Failed to load model 'path/to/model.onnx'"
Ensure the model path is correct relative to the server's working directory (where you run `cargo run` or `skardi-server`).

### "onnx_predict requires at least 2 arguments"
The UDF needs at minimum the model path and one input. Check your SQL syntax.

### "Unsupported data type for integer conversion"
For integer-input models, ensure inputs are cast to `BIGINT` or `INT`:
```sql
onnx_predict('model.onnx', CAST(col AS BIGINT), ...)
```

### "ORT run failed"
This usually means the input shape or type doesn't match what the model expects. Enable debug logging to see the model's expected inputs:
```bash
RUST_LOG=debug cargo run --bin skardi-server -- ...
```

The logs will show:
```
Model input 'user_id': type=Int64, shape=[-1, 1]
Model input 'item_id': type=Int64, shape=[-1, 1]
```
