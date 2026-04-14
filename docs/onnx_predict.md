# ONNX Inference (`onnx_predict`)

> **Requires `--features embedding`** — build with:
> ```bash
> cargo build --release -p skardi-server --features embedding
> ```

`onnx_predict` is a DataFusion scalar UDF that runs ONNX model inference directly inside SQL queries. Models are loaded lazily on first use and cached in memory — no pre-registration or configuration required.

## Function Signature

```sql
onnx_predict(model_path, input1, input2, ...) -> FLOAT | LIST(FLOAT)
```

| Argument | Type | Description |
|----------|------|-------------|
| `model_path` | string literal | Path to the `.onnx` file, relative to the server's working directory |
| `input1..N` | any | Model inputs — types and count must match the model's expected inputs |

**Returns:**
- `FLOAT` — one scalar prediction per row (default)
- `LIST(FLOAT)` — when any input is a `ListArray` (e.g., from `array_agg`), output preserves row cardinality

## Key Behaviors

- **Lazy loading** — the ONNX runtime (ORT) initializes on first model load; models are cached by file path
- **Automatic type detection** — input/output types and shapes are introspected from the model metadata
- **Integer-input models** — models expecting `INT64` inputs (e.g., ID-based NCF) are processed row-by-row
- **Float-input models** — models with float inputs are batched into ndarrays for efficiency
- **List mode** — aggregated inputs (from `array_agg`) switch the output to `LIST(FLOAT)`

## Examples

### ID-based scoring (e.g., recommendation)

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

### Feature-based regression

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

### Time-series forecasting (list mode)

```sql
SELECT
  group_id,
  onnx_predict('models/forecaster.onnx',
    array_agg(value ORDER BY timestamp)
  ) AS forecast
FROM time_series
GROUP BY group_id
```

## Troubleshooting

**"Failed to load model"** — check the model path is correct relative to the working directory.

**"onnx_predict requires at least 2 arguments"** — the UDF needs at minimum the model path and one input.

**"Unsupported data type for integer conversion"** — cast inputs explicitly:
```sql
onnx_predict('model.onnx', CAST(col AS BIGINT), ...)
```

**"ORT run failed"** — input shape or type mismatch. Enable debug logging to inspect expected inputs:
```bash
RUST_LOG=debug cargo run --bin skardi-server --features embedding -- ...
```

## Demo

See [demo/movie_recommendation/](../demo/movie_recommendation/) for a complete end-to-end example combining Lance KNN search with `onnx_predict` for personalized movie recommendations.
