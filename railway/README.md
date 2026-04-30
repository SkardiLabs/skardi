# Deploy and Host Skardi with Railway

[Skardi](https://github.com/SkardiLabs/skardi) is an open-source data plane for AI agents — a single-node engine that turns parameterized SQL into REST endpoints (and shell verbs) over Postgres, MySQL, SQLite, MongoDB, Redis, Lance, Iceberg, S3 / GCS / Azure, and more. This template deploys Skardi v0.3.0 on Railway with a working sample backend (SQLite + four CRUD pipelines) so you can call it from Claude Code, Cursor, or any agent runtime within minutes.

## About hosting Skardi

Skardi runs as `skardi-server`, a Rust binary published as `ghcr.io/skardilabs/skardi/skardi-server:0.3.0`. It listens on a single HTTP port, serves the Skardi dashboard at `/`, exposes a liveness probe at `/health` and per-pipeline readiness at `/health/:name`, and turns every YAML file in its `--pipeline` directory into a `POST /<name>/execute` endpoint. State lives wherever the registered data sources live — for this template, a SQLite database on a Railway volume mounted at `/data`. Swap the SQLite source in `ctx.yaml` for a Railway-managed Postgres / MySQL and Skardi will federate against it the same way. Skardi also supports session auth via [better-auth](https://www.better-auth.com), OpenTelemetry traces / metrics / logs, and inline embeddings / ONNX inference under a build flag — all without any application server code.

## Common use cases

- Zero-code REST backend for an AI agent (this template's default — four CRUD endpoints over SQLite).
- RAG / hybrid search service over Postgres (pgvector + FTS), Lance, or SeekDB.
- Federated analytics: JOIN a CSV in S3 against a Railway Postgres against a SQLite cache in one query.
- Memory / session store for an agent loop, with TTL and provenance columns.
- A safe SQL gateway in front of a production database — pipelines act as parameterized, read-only views.

## Dependencies for hosting Skardi

- A Railway volume mounted at `/data` (persists the SQLite database across redeploys; not required if you swap to Postgres).
- The public Skardi container image — `ghcr.io/skardilabs/skardi/skardi-server:0.3.0`. No registry auth needed.
- Optionally, a Railway-managed Postgres / MySQL / Redis if you change `ctx.yaml` to use one.

### Deployment dependencies

- [Skardi GitHub repo](https://github.com/SkardiLabs/skardi) — source for the published image.
- [Skardi pipeline reference](https://github.com/SkardiLabs/skardi/blob/main/docs/pipelines.md) — to author your own endpoints.
- [Skardi context reference](https://github.com/SkardiLabs/skardi/blob/main/docs/server.md) — to wire up extra data sources.
- [Railway template docs](https://docs.railway.com/templates/create) — for forking and republishing this template.

### Implementation details

The template is a thin layer on top of the official image. The `Dockerfile` does two things:

1. A `seed` build stage installs `sqlite3` and bakes `seed.sql` into a starter database.
2. The runtime stage extends `skardi-server:0.3.0`, copies in `ctx.yaml`, the `pipelines/` directory, the seed database, and an `entrypoint.sh` that copies the seed to `/data/backend.db` on first boot and then runs:

```sh
exec skardi-server \
    --ctx /app/ctx.yaml \
    --pipeline /app/pipelines \
    --port "$PORT"
```

`railway.json` requests Railway's Dockerfile builder, points it at `railway/Dockerfile`, sets `/health` as the healthcheck, and uses an `ON_FAILURE` restart policy with five retries.

To add an endpoint, drop a YAML pipeline into `railway/pipelines/`:

```yaml
kind: pipeline
metadata:
  name: "list-tasks"
  version: "1.0.0"
  description: "List a user's tasks."
spec:
  query: |
    SELECT id, title, done, created_at
    FROM tasks
    WHERE user_id = {user_id}
    ORDER BY created_at DESC
```

Redeploy and `POST /list-tasks/execute` with `{"user_id": 1}` works.

#### Recommended service variables

When forking this template into the Railway composer, declare variables with descriptions and use template functions for secrets — do not hardcode credentials:

| Variable | Required | Suggested value | Description |
|---|---|---|---|
| `PORT` | Yes (auto) | injected by Railway | HTTP port the server binds to. |
| `DATA_DIR` | No | `/data` | Volume mount path for the SQLite database. |
| `AUTH_MODE` | No | `BETTER_AUTH_DIESEL_SQLITE` | Enables session auth. Omit to leave endpoints open. |
| `AUTH_SECRET` | If auth is on | `${{ secret(48) }}` | Signing key for sessions. Use Railway's `secret()` template function so it's generated once at deploy and never visible to template users. |
| `AUTH_DB_PATH` | If auth is on | `/data/auth.db` | Stores the auth schema on the persistent volume. |

If you wire up a Railway-managed Postgres alongside this service, reference it with private-network variables — never the public host:

```yaml
# railway/ctx.yaml (example for a Postgres swap)
spec:
  data_sources:
    - name: tasks
      type: postgres
      access_mode: read_write
      connection_string: "postgresql://${{ Postgres.RAILWAY_PRIVATE_DOMAIN }}:5432/${{ Postgres.PGDATABASE }}?sslmode=disable"
      options:
        table: tasks
        schema: public
        user_env: PG_USER
        pass_env: PG_PASSWORD
```

`RAILWAY_PRIVATE_DOMAIN` keeps service-to-service traffic on Railway's private network — faster, free egress, and never exposed to the public internet.

### Why deploy Skardi on Railway

- **One platform for the agent's whole data plane.** Spin up Skardi alongside Railway's managed Postgres, MySQL, or Redis, and federate them in one SQL — no glue services to host elsewhere.
- **Private networking by default.** `RAILWAY_PRIVATE_DOMAIN` keeps the SQL hop between Skardi and your data store off the public internet.
- **Volumes for stateful demos.** SQLite state (and auth / job-ledger databases) survive redeploys with one mount.
- **Healthcheck-aware deploys.** `/health` is wired up so Railway only routes traffic once the engine has registered its sources.
- **Fork-friendly.** This entire template is plain YAML and a Dockerfile — every endpoint is editable in the dashboard's file editor or via PR.

## Quick local verification

Sanity-check the same image Railway will build:

```bash
docker build -f railway/Dockerfile -t skardi-railway:0.3.0 railway/

docker run --rm -e PORT=8080 -p 8080:8080 \
  -v skardi-railway-data:/data \
  skardi-railway:0.3.0

curl -s http://localhost:8080/health | jq .
curl -s -X POST http://localhost:8080/list-tasks/execute \
  -H "Content-Type: application/json" -d '{"user_id": 1}' | jq .
curl -s -X POST http://localhost:8080/create-task/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "title": "Ship Railway template"}' | jq .
```

## Submitting this template to Railway

1. Deploy `railway/` on Railway (New Project → Deploy from GitHub repo, point config-as-code to `railway/railway.json`). Attach a volume mounted at `/data` and generate a public domain.
2. Open **Settings → Generate Template from Project**.
3. In the composer, set:
   - **Name:** `Skardi`
   - **Description:** "Agent data plane — declarative SQL pipelines as parameterized REST endpoints. Federates Postgres, MySQL, SQLite, MongoDB, Redis, Lance, S3, and more in one query."
   - **Tags:** `ai`, `agents`, `data`, `sql`, `rag`, `vector-search`
   - **Icon:** upload `asset/logo.png` from the repo (1:1, transparent background).
   - **Source:** GitHub repo `SkardiLabs/skardi`, root directory `railway/`.
   - **Variable descriptions:** copy from the table above so every variable is documented in the marketplace UI.
4. Click **Create Template**, then submit the share URL via [Railway's partner program](https://railway.com/partners) for marketplace listing.
