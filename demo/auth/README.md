# Skardi Auth Demo: End-to-End Authentication

This demo walks through enabling authentication on the Skardi server and using it to protect production pipelines. It uses the built-in **BetterAuth in-memory** mode, which is ideal for demos, staging, and development — no external database required.

## How Auth Works

When auth is enabled:
- `POST /api/auth/sign-up/email` and `POST /api/auth/sign-in/email` are exposed for registration and login.
- Every `/:pipeline/execute` call must carry a valid session token, either as an `Authorization: Bearer <token>` header or a session cookie.
- Auth users and sessions are queryable as virtual SQL tables (`auth.users`, `auth.sessions`), so you can JOIN them with your own data in pipelines.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AUTH_MODE` | Yes | Set to `BETTER_AUTH_IN_MEMORY` to enable auth |
| `AUTH_SECRET` | Yes | Secret key used to sign sessions (32+ characters) |
| `AUTH_BASE_URL` | No | Public base URL of the server (defaults to `http://localhost:<PORT>`) |

## Prerequisites

Build the server binary:

```bash
cargo build --bin skardi-server
```

## Step 1 — Start the Server with Auth Enabled

From the project root, start the server with the product-search demo pipeline and auth turned on:

```bash
AUTH_MODE=BETTER_AUTH_IN_MEMORY \
AUTH_SECRET="super-secret-key-at-least-32-characters-long" \
cargo run --bin skardi-server -- \
  --pipeline demo/pipeline.yaml \
  --ctx demo/ctx.yaml \
  --port 8080
```

You should see the server start and list the auth routes alongside the pipeline endpoint:

```
Starting Skardi Online Serving Pipeline Server
...
Server listening on 0.0.0.0:8080
```

## Step 2 — Sign Up

Register a new user account:

```bash
curl -s -X POST http://localhost:8080/api/auth/sign-up/email \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Alice",
    "email": "alice@example.com",
    "password": "mysecretpassword"
  }' | jq .
```

**Response:**
```json
{
  "token": "sess_abc123...",
  "user": {
    "id": "usr_xyz...",
    "name": "Alice",
    "email": "alice@example.com",
    "createdAt": "2025-01-15T12:00:00.000Z"
  }
}
```

Save the `token` value — you will use it to authenticate pipeline requests.

## Step 3 — Sign In (for Returning Users)

If you already have an account, sign in to get a new session token:

```bash
curl -s -X POST http://localhost:8080/api/auth/sign-in/email \
  -H "Content-Type: application/json" \
  -d '{
    "email": "alice@example.com",
    "password": "mysecretpassword"
  }' | jq .
```

**Response:**
```json
{
  "token": "sess_abc123...",
  "user": {
    "id": "usr_xyz...",
    "email": "alice@example.com"
  }
}
```

## Step 4 — Execute a Pipeline with the Session Token

With auth enabled, unauthenticated requests are rejected:

```bash
# Without a token — returns 401
curl -s -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 3}' | jq .
```

```json
{"error": "Authentication required"}
```

Add the `Authorization: Bearer` header to authenticate:

```bash
TOKEN="sess_abc123..."   # replace with your actual token

curl -s -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "brand": null,
    "min_price": null,
    "max_price": 50.0,
    "color": null,
    "category": null,
    "availability": null,
    "limit": 5
  }' | jq .
```

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "product_id": 13,
      "product_name": "Scooter Bicycle Oven",
      "brand": "Mclean-Aguilar",
      "category": "Laptops & Computers",
      "price": 1,
      "currency": "USD",
      "availability_status": "in_stock"
    }
  ],
  "row_count": 5,
  "execution_time_ms": 12
}
```

## Step 5 — Use a Session Cookie Instead (Optional)

BetterAuth also sets a session cookie on sign-in. You can pass it along in requests as an alternative to the `Authorization` header:

```bash
# Sign in and capture the Set-Cookie header
curl -s -c cookies.txt -X POST http://localhost:8080/api/auth/sign-in/email \
  -H "Content-Type: application/json" \
  -d '{"email": "alice@example.com", "password": "mysecretpassword"}'

# Use the saved cookie for pipeline execution
curl -s -b cookies.txt -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"limit": 3}' | jq .
```

## Step 6 — Query Auth Tables from SQL (Advanced)

When auth is enabled, two virtual tables are available inside any pipeline query:

- **`auth.users`** — all registered users (id, name, email, role, created_at, …)
- **`auth.sessions`** — all active sessions (id, token, user_id, expires_at, …)

You can JOIN these with your own data sources. For example, create `demo/auth/pipelines/active-users.yaml`:

```yaml
metadata:
  name: active-users
  version: 1.0.0
  description: Lists users who currently have an active session

query: |
  SELECT
    u.id,
    u.name,
    u.email,
    s.expires_at
  FROM auth.users u
  JOIN auth.sessions s ON s.user_id = u.id
  WHERE s.expires_at > NOW()
  LIMIT {limit}
```

Start the server with this pipeline alongside the products pipeline:

```bash
AUTH_MODE=BETTER_AUTH_IN_MEMORY \
AUTH_SECRET="super-secret-key-at-least-32-characters-long" \
cargo run --bin skardi-server -- \
  --pipeline demo/auth/pipelines/active-users.yaml \
  --ctx demo/ctx.yaml \
  --port 8080
```

Execute it (requires a valid session token):

```bash
curl -s -X POST http://localhost:8080/active-users/execute \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"limit": 10}' | jq .
```

## Full End-to-End Shell Script

Save this as a convenience script to try everything in one go:

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8080"

echo "==> Sign up"
RESPONSE=$(curl -s -X POST "$BASE/api/auth/sign-up/email" \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","email":"alice@example.com","password":"mysecretpassword"}')
echo "$RESPONSE" | jq .

TOKEN=$(echo "$RESPONSE" | jq -r '.token')
echo ""
echo "==> Session token: $TOKEN"
echo ""

echo "==> Execute pipeline (authenticated)"
curl -s -X POST "$BASE/product-search-demo/execute" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"max_price": 100.0, "limit": 3}' | jq .
```

## Auth API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/auth/sign-up/email` | POST | Register a new user |
| `/api/auth/sign-in/email` | POST | Sign in and obtain a session token |
| `/api/auth/sign-out` | POST | Invalidate the current session |
| `/api/auth/session` | GET | Retrieve the current session (requires token) |

### Sign-Up / Sign-In Request Body

```json
{
  "name": "Alice",
  "email": "alice@example.com",
  "password": "mysecretpassword"
}
```

(`name` is only required on sign-up)

### Authentication Header

```
Authorization: Bearer <session-token>
```

### Error Responses

| Status | Meaning |
|--------|---------|
| `401 Unauthorized` | No token provided |
| `401 Unauthorized` | Token is invalid or expired |

```json
{"error": "Authentication required"}
{"error": "Invalid or expired session"}
```

## Production Notes

- `BETTER_AUTH_IN_MEMORY` stores all users and sessions in RAM — data is lost on restart. For production, a persistent database adapter is recommended.
- Set `AUTH_BASE_URL` to your server's public URL so that cookies and redirects work correctly behind a reverse proxy.
- Use a strong, randomly generated `AUTH_SECRET` (at minimum 32 characters). You can generate one with: `openssl rand -base64 32`
- Session tokens are short-lived. Clients should re-authenticate when they receive a `401` response.
