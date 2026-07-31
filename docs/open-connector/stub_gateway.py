#!/usr/bin/env python3
"""A tiny Open Connector stand-in for the GitHub source-pack demo.

Plays the role DynamoDB Local plays in the DynamoDB demo: a local,
credential-free substitute for the remote service, so every command in the
README runs offline. It implements just enough of the gateway's /v1
contract for the `github` pack:

    GET  /v1/health
    GET  /v1/actions/<action_id>
    POST /v1/actions/<action_id>

with the real gateway's uniform response envelope
(`{"success": …, "message": …, "data": …, "meta": …}`), and serves a
fictional `acme/widgets` repository with GitHub-shaped rows (page-number
pagination via `page`/`perPage`, `state` filtering, inclusive `since`).
Any non-empty Bearer token is accepted. Standard library only.

Run against a real Open Connector deployment instead by following the
"Running against a real Open Connector gateway" section of the README.
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 3000

ISSUES = [
    {
        "id": 101, "number": 1, "title": "Scan panics on empty page", "state": "open",
        "body": "Steps to reproduce: run a scan against an empty collection.",
        "user": {"login": "octocat"},
        "assignees": [{"login": "octocat"}, {"login": "hubot"}],
        "labels": [{"name": "bug"}, {"name": "p1"}],
        "comments": 3,
        "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T10:30:00Z",
        "closed_at": None,
        "html_url": "https://github.com/acme/widgets/issues/1",
    },
    {
        "id": 102, "number": 2, "title": "Docs typo in quick start", "state": "closed",
        "body": None,
        "user": None,  # the GitHub "ghost" user
        "assignees": [], "labels": [], "comments": 0,
        "created_at": "2026-01-03T00:00:00Z", "updated_at": "2026-01-04T00:00:00Z",
        "closed_at": "2026-01-04T00:00:00Z",
        "html_url": "https://github.com/acme/widgets/issues/2",
    },
    {
        # Pure issues only: the real Open Connector action filters out the
        # pull requests GitHub's raw issues endpoint would mix in.
        "id": 103, "number": 3, "title": "Add dark mode", "state": "open",
        "body": "Tracking issue for the dark palette.",
        "user": {"login": "hubot"},
        "assignees": [], "labels": [{"name": "enhancement"}], "comments": 1,
        "created_at": "2026-01-05T00:00:00Z", "updated_at": "2026-01-06T00:00:00Z",
        "closed_at": None,
        "html_url": "https://github.com/acme/widgets/issues/3",
    },
    {
        "id": 104, "number": 4, "title": "Flaky retry test", "state": "open",
        "body": None,
        "user": {"login": "octocat"},
        "assignees": [{"login": "hubot"}], "labels": [{"name": "bug"}], "comments": 7,
        "created_at": "2026-01-07T00:00:00Z", "updated_at": "2026-01-08T00:00:00Z",
        "closed_at": None,
        "html_url": "https://github.com/acme/widgets/issues/4",
    },
    {
        "id": 105, "number": 5, "title": "Bump arrow to 54", "state": "closed",
        "body": "Routine dependency bump.",
        "user": {"login": "dependabot"},
        "assignees": [], "labels": [{"name": "dependencies"}], "comments": 0,
        "created_at": "2026-01-09T00:00:00Z", "updated_at": "2026-01-10T00:00:00Z",
        "closed_at": "2026-01-10T00:00:00Z",
        "html_url": "https://github.com/acme/widgets/issues/5",
    },
]

PULL_REQUESTS = [
    {
        "id": 201, "number": 3, "title": "Add dark mode", "state": "open",
        "body": "Implements the dark palette.",
        "user": {"login": "hubot"}, "draft": False,
        "head": {"ref": "feature/dark-mode"}, "base": {"ref": "main"},
        "created_at": "2026-01-05T00:00:00Z", "updated_at": "2026-01-06T00:00:00Z",
        "closed_at": None, "merged_at": None,
        "html_url": "https://github.com/acme/widgets/pull/3",
    },
    {
        "id": 202, "number": 6, "title": "Refactor pagination", "state": "closed",
        "body": None,
        "user": {"login": "octocat"}, "draft": False,
        "head": {"ref": "refactor/pagination"}, "base": {"ref": "main"},
        "created_at": "2026-01-11T00:00:00Z", "updated_at": "2026-01-12T00:00:00Z",
        "closed_at": "2026-01-12T00:00:00Z", "merged_at": "2026-01-12T00:00:00Z",
        "html_url": "https://github.com/acme/widgets/pull/6",
    },
]

# Output JSON Schemas returned by discovery. The issues schema is complete
# enough for `open_connector_scan` to derive a deterministic row type at
# `$.issues`; declared primitives become typed columns, the rest opaque JSON.
ISSUES_SCHEMA = {
    "type": "object",
    "properties": {
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "number": {"type": "integer"},
                    "title": {"type": "string"},
                    "state": {"type": "string"},
                    "body": {"type": ["string", "null"]},
                    "user": {"type": "object"},
                    "assignees": {"type": "array"},
                    "labels": {"type": "array"},
                    "comments": {"type": "integer"},
                    "created_at": {"type": "string"},
                    "updated_at": {"type": "string"},
                    "closed_at": {"type": ["string", "null"]},
                    "html_url": {"type": "string"},
                },
            },
        }
    },
}
PULLS_SCHEMA = {"type": "object", "properties": {"pull_requests": {"type": "array"}}}

ACTIONS = {
    "github.list_repository_issues": ("issues", ISSUES, ISSUES_SCHEMA),
    "github.list_pull_requests": ("pull_requests", PULL_REQUESTS, PULLS_SCHEMA),
}


def execute(action_id, params):
    row_key, rows, _ = ACTIONS[action_id]
    state = params.get("state")
    if state in ("open", "closed"):
        rows = [r for r in rows if r["state"] == state]
    since = params.get("since")
    if since:  # inclusive, like GitHub's issues `since`
        rows = [r for r in rows if r["updated_at"] >= since]
    page = int(params.get("page", 1))
    # Open Connector action inputs are camelCase (and strict about it).
    per_page = int(params.get("perPage", 30))
    sliced = rows[(page - 1) * per_page : page * per_page]
    # The raw page length before any post-pagination filtering (the stub
    # does none), matching open-connector#228's pageInfo signal that the
    # issues table paginates on.
    return {row_key: sliced, "pageInfo": {"fetched": len(sliced)}}


class Handler(BaseHTTPRequestHandler):
    def _reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _ok(self, data):
        # The gateway's uniform success envelope.
        self._reply(200, {"success": True, "message": "OK", "data": data, "meta": {}})

    def _err(self, status, error_code, message):
        # The gateway's uniform failure envelope.
        self._reply(status, {
            "success": False, "message": message, "data": None,
            "errorCode": error_code, "meta": {},
        })

    def _authorized(self):
        header = self.headers.get("authorization", "")
        if header.startswith("Bearer ") and len(header) > len("Bearer "):
            return True
        self._err(401, "unauthorized", "missing runtime token")
        return False

    def do_GET(self):
        if not self._authorized():
            return
        if self.path == "/v1/health":
            return self._ok({"ok": True})
        if self.path.startswith("/v1/actions/"):
            action_id = self.path.removeprefix("/v1/actions/")
            if action_id in ACTIONS:
                _, _, schema = ACTIONS[action_id]
                return self._ok({
                    "inputSchema": {},
                    "outputSchema": schema,
                    # The real gateway publishes no readOnly classification
                    # (yet); the stub does, so the raw-scan demo can run.
                    "execution": {"locallyExecutable": True, "readOnly": True},
                })
        self._err(404, "not_found", "unknown action")

    def do_POST(self):
        if not self._authorized():
            return
        prefix = "/v1/actions/"
        if self.path.startswith(prefix):
            action_id = self.path.removeprefix(prefix)
            if action_id in ACTIONS:
                length = int(self.headers.get("content-length", 0))
                envelope = json.loads(self.rfile.read(length) or b"{}")
                params = envelope.get("input", {})
                return self._ok(execute(action_id, params))
        self._err(404, "not_found", "unknown action")

    def log_message(self, fmt, *args):  # quieter demo output
        print(f"[stub-gateway] {self.command} {self.path}")


if __name__ == "__main__":
    print(f"[stub-gateway] serving acme/widgets on http://127.0.0.1:{PORT}")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
