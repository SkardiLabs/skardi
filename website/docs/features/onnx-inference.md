---
sidebar_position: 2
---

## ONNX Model Inference

> **Note:** ONNX support is behind a feature flag. Build with `--features onnx` to enable it:
> ```bash
> cargo build --release -p skardi-server --features onnx
> ```

Run ONNX model predictions directly in SQL using the `onnx_predict` scalar UDF. Models are loaded lazily on first use and cached in memory.

```sql
onnx_predict('path/to/model.onnx', input1, input2, ...) -> FLOAT
```

- First argument: path to an `.onnx` file (relative to the server's working directory)
- Remaining arguments: model inputs (types are auto-detected from the ONNX model)
- Returns: `FLOAT` per row, or `LIST(FLOAT)` when inputs are aggregated lists

Example — score candidates with a Neural Collaborative Filtering model:

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

Pre-built models are available in the `models/` directory (`ncf.onnx`, `TinyTimeMixer.onnx`).

For the full guide including the movie recommendation demo, see [demo/onnx_predict/README.md](https://github.com/SkardiLabs/skardi/blob/main/demo/onnx_predict/README.md).
