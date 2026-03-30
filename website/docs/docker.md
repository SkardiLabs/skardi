---
sidebar_position: 8
---

## Docker

### Build the image

```bash
docker build -t skardi .

# With ONNX support
docker build -t skardi --build-arg FEATURES=onnx .
```

### Run with config files mounted

```bash
docker run --rm \
  -v /path/to/your/ctx.yaml:/config/ctx.yaml \
  -v /path/to/your/pipeline.yaml:/config/pipeline.yaml \
  -p 8080:8080 \
  skardi \
  --ctx /config/ctx.yaml \
  --pipeline /config/pipeline.yaml \
  --port 8080
```

Mount an entire directory of pipeline files:

```bash
docker run --rm \
  -v /path/to/your/ctx.yaml:/config/ctx.yaml \
  -v /path/to/your/pipelines:/config/pipelines \
  -p 8080:8080 \
  skardi \
  --ctx /config/ctx.yaml \
  --pipeline /config/pipelines \
  --port 8080
```
