# Simple Backend Demo

This demo shows how to use Skardi as a zero-code REST backend. A SQLite database provides persistence; YAML pipeline files define every endpoint. No application server, no ORM, no glue code.

## What's included

| Pipeline | Endpoint | Description |
|----------|----------|-------------|
| `list_tasks.yaml` | `POST /list-tasks/execute` | List a user's tasks |
| `create_task.yaml` | `POST /create-task/execute` | Create a new task |
| `complete_task.yaml` | `POST /complete-task/execute` | Mark a task done |
| `delete_task.yaml` | `POST /delete-task/execute` | Delete a task |

## Quick Start

```bash
# 1. Create the database and seed data
bash demo/simple_backend/setup.sh

# 2. Start the server
cargo run --bin skardi-server -- \
  --ctx demo/simple_backend/ctx.yaml \
  --pipeline demo/simple_backend/pipelines/ \
  --port 8080
```

## Example Requests

```bash
# List tasks for user 1
curl -X POST http://localhost:8080/list-tasks/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1}' | jq .

# Create a new task
curl -X POST http://localhost:8080/create-task/execute \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "title": "Review PR"}' | jq .

# Mark task 1 as done
curl -X POST http://localhost:8080/complete-task/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 1}' | jq .

# Delete task 2
curl -X POST http://localhost:8080/delete-task/execute \
  -H "Content-Type: application/json" \
  -d '{"id": 2}' | jq .
```

## Adding Authentication

Protect the endpoints by enabling better-auth. Add the following environment variables before starting the server:

```bash
export AUTH_MODE=BETTER_AUTH_DIESEL_SQLITE
export AUTH_SECRET=your-secret-key-at-least-32-chars
export AUTH_DB_PATH=demo/simple_backend/auth.db

cargo run --bin skardi-server -- \
  --ctx demo/simple_backend/ctx.yaml \
  --pipeline demo/simple_backend/pipelines/ \
  --port 8080
```

Then register and sign in:

```bash
# Register
curl -X POST http://localhost:8080/api/auth/sign-up/email \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "secret", "name": "Alice"}' | jq .

# Sign in — save the session token
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/sign-in/email \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "secret"}' | jq -r '.token')

# Use the token on every pipeline call
curl -X POST http://localhost:8080/list-tasks/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"user_id": 1}' | jq .
```

See [docs/auth/README.md](../../docs/auth/README.md) for the full authentication guide.
