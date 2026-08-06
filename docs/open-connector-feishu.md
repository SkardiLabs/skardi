# Feishu Source Pack

The built-in `feishu` source pack exposes a Feishu user's own workspace —
chats and their messages and members, tasks, and wiki spaces/nodes — as
stable SQL tables through an
[Open Connector gateway](open-connector.md). Feishu credentials are an
OAuth **user_access_token** obtained through the gateway's OAuth flow
against a user-provided Feishu custom app; Skardi holds only the gateway
runtime token. Visibility is exactly the authorizing user's: their chats,
their tasks, the wiki they can read.

**The wire contract is Open Connector's HYBRID shape**: every feishu list
executor rebuilds the pagination envelope (camelCase `$.items` /
`$.pageToken` / `$.hasMore`, with the provider's `page_token` normalized
to a null `pageToken` at end-of-collection) while passing the Feishu
API's item objects through **raw** — snake_case keys, and timestamps as
epoch **digit strings** (milliseconds for im, seconds for wiki), which is
what the `timestamp_ms_string_utc` / `timestamp_s_string_utc` column
types decode. Reconciled against a live gateway (v1.3.3).

> **Live-verified (2026-08-04):** all six tables are reconciled against
> a real workspace end to end — registration through live discovery,
> real scans (86 messages over two real cursor pages, zero duplicate
> ids; `create_time >=` pushdown narrowing a live scan; wiki's
> non-empty final token terminating cleanly), and every mapped column
> non-NULL on real rows. The gateway declares every feishu items schema
> loose (no declared properties), so no column is protected by the
> fingerprint gate — real rows are the column truth, and the bundled
> fixtures are redacted live captures.

## Binding

```yaml
spec:
  data_sources:
    - name: saas
      type: open_connector
      connection_string: http://open-connector:3000
      hierarchy_level: catalog

      open_connector:
        runtime_token_env: OPEN_CONNECTOR_TOKEN
        bindings:
          # The three tables that need NO resource — a first ctx can only
          # cover these; the other three each require an id you get by
          # querying chats / wiki_spaces first, then coming back.
          - name: team               # schema name in SQL
            source_pack: feishu
            tables: [chats, tasks, wiki_spaces]
          # Each of the remaining tables needs one resource, so each
          # needs its own binding — adding them to `team` fails startup
          # with `missing required resource input`.
          - name: standup            # per-chat binding for chat history
            source_pack: feishu
            resource:
              containerId: oc_a1b2c3d4e5f6   # chat_id from the chats table
            tables: [messages]
          - name: standup_members
            source_pack: feishu
            resource:
              chatId: oc_a1b2c3d4e5f6        # chat_id from the chats table
            tables: [chat_members]
          - name: handbook
            source_pack: feishu
            resource:
              spaceId: "7034502641455497244" # space_id from wiki_spaces
            tables: [wiki_nodes]
```

```sql
SELECT name, external FROM saas.team.chats ORDER BY name;

SELECT content, create_time
FROM saas.standup.messages
WHERE create_time >= TIMESTAMP '2026-07-01T00:00:00Z'
  AND msg_type = 'text'
ORDER BY create_time;

-- The same definition, ad hoc, without a binding:
SELECT member_id, name
FROM open_connector_query('saas', 'feishu.chat_members',
                          '{"chatId":"oc_a1b2c3d4e5f6"}');
```

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `chats` | `feishu.list_chats` | — | cursor, 100/page | — |
| `messages` | `feishu.list_messages` | `containerId` (required) | cursor, 50/page | `create_time >=` → `startTime` (inexact) |
| `chat_members` | `feishu.list_chat_members` | `chatId` (required) | cursor, 100/page | — |
| `tasks` | `feishu.list_tasks` | — | cursor, 100/page | — |
| `wiki_spaces` | `feishu.list_wiki_spaces` | — | cursor, 50/page | — |
| `wiki_nodes` | `feishu.list_wiki_nodes` | `spaceId` (required), `parentNodeToken` (optional) | cursor, 50/page | — |

Design notes:

- **`chats` and `messages` pin `sortType: ByCreateTimeAsc`** — the API's
  activity-ordered default reshuffles rows while a scan pages, which can
  skip or duplicate rows; creation order is immutable, so the cursor is
  stable.
- **`messages` is chat history for ONE chat** (`containerIdType` pinned
  to `chat`; per-chat binding, the shape Notion's `block_children`
  established). The `create_time >=` pushdown renders inclusive epoch
  seconds as a digit string (`startTime`); `endTime` is deliberately
  unmapped — it is exclusive, and flooring an upper bound would drop
  rows. `body.content` maps to the `content` column as the provider's
  own JSON-encoded payload text (its inner schema varies by `msg_type`).
- **`tasks` sends `type: my_tasks`** — the only value Feishu accepts
  (`1470400: Invalid Param 'type'. Only 'my_tasks' is supported.` for
  `assigned`/`created`/`followed`), not a tunable choice — and always
  omits the `completed` input, Feishu's spelling of a state=all listing. Nothing pushes it:
  real rows carry no `completed` boolean (completion on the wire is
  `status: todo|done` plus `completed_at`), so filter on `status`
  locally.
- **`wiki_nodes` lists ONE level**: the children of `parentNodeToken`,
  or the space root when omitted. Walking a whole space is client-side
  recursion over `has_child` / `node_token`.
- No table declares `error_path`: the gateway's executors consume
  Feishu's in-band `code != 0` envelope and return a failure envelope
  themselves.

## Authorization and visibility

The gateway's feishu provider uses the OAuth authorization-code flow
(`authTypes: ["oauth2"]`) against a Feishu custom app the operator
creates. Rows are the authorizing user's view — a chat the user left or
a wiki space they cannot read is simply absent, not an error.

**Gateway version is a floor, not a fact**: the six actions this pack
needs were added to Open Connector after older mid-2025 builds (which
expose only docs/bitable feishu actions); a too-old gateway fails
registration with `action 'feishu.list_chats' was not found`, which
reads like a typo but means "upgrade the gateway". Self-check before
going further:

```bash
curl -s -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" \
  "$GATEWAY/v1/actions?service=feishu&limit=500" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); \
    ids=[i['id'] for i in d['data']['items']]; print(len(ids)); \
    print([n for n in ['feishu.list_chats','feishu.list_messages','feishu.list_chat_members','feishu.list_tasks','feishu.list_wiki_spaces','feishu.list_wiki_nodes'] if n not in ids])"
# expect: a few hundred actions, then an empty list []
```

Operational findings from the live verification, all three of which the
Feishu console gates independently of each other:

- The `messages` table requires the **`im:message:readonly`** scope (or
  `im:message` / `im:message.history:readonly`) — the
  `im:message.*.get_as_user` scopes the gateway's action metadata
  declares are NOT honored for the user-identity read path (Feishu
  99991679 names the real set).
- The im tables additionally require the app's **bot capability**
  (Feishu 232025), even though every read runs as the user.
- Feishu's `im/v1/messages` caps `page_size` at **50** on the wire
  (99992402 above it) despite the gateway schema declaring 100 — the
  pack requests 50.
- Upstream gateway caveat: its authorization URL requests the union of
  ALL feishu actions' scopes with no narrowing surface — measured at
  164 scopes on the live authorize URL, including destructive write
  scopes, to read six tables — which Feishu rejects (20027) unless the
  app enables every one. Until upstream grows a config-level override
  ([#267](https://github.com/oomol-lab/open-connector/issues/267)),
  narrow `feishuOAuthScopes` in the gateway's
  `src/providers/feishu/definition.ts` to what these tables need:

  ```ts
  const feishuOAuthScopes = [
    "offline_access",
    "im:chat:read",                       // chats
    "im:chat.members:read",               // chat_members
    "im:message:readonly",                // messages (next three also messages)
    "im:message.group_msg:get_as_user",
    "im:message.p2p_msg:get_as_user",
    "im:message.reactions:read",
    "task:task:read",                     // tasks
    "wiki:space:retrieve",                // wiki_spaces
    "wiki:node:retrieve",                 // wiki_nodes
  ];
  ```

  The Feishu console bulk-imports scopes, so enabling them is one paste:

  ```json
  {"scopes":{"tenant":[],"user":["offline_access","im:chat:read","im:chat.members:read",
  "im:message:readonly","im:message.group_msg:get_as_user","im:message.p2p_msg:get_as_user",
  "im:message.reactions:read","task:task:read","wiki:space:retrieve","wiki:node:retrieve"]}}
  ```

- Zero-trust corporate VPNs (aTrust / EasyConnect class) that map
  external domains into `198.18.0.0/15` trip the gateway's egress guard
  BEFORE any request leaves the process: the OAuth exchange fails in
  tens of milliseconds with only `oauth_token_exchange_failed` and no
  resolved address — the speed is the tell. The guard checks reserved
  ranges unconditionally (`OOMOL_CONNECT_ALLOW_PRIVATE_NETWORK` cannot
  open them, and it does not reach provider egress at all); tracked
  upstream as
  [#275](https://github.com/oomol-lab/open-connector/issues/275).

Filed upstream (oomol-lab/open-connector) from this verification pass:
[#267](https://github.com/oomol-lab/open-connector/issues/267)
(scope-union OAuth URL),
[#268](https://github.com/oomol-lab/open-connector/issues/268)
(get_as_user scopes not honored for user-identity reads),
[#269](https://github.com/oomol-lab/open-connector/issues/269)
(declared pageSize 100 vs the wire's 50 cap, fix in
[PR #271](https://github.com/oomol-lab/open-connector/pull/271)), and
[#270](https://github.com/oomol-lab/open-connector/issues/270)
(wiki's non-empty final page_token beside has_more:false).

`message_position` (a digit string on every live message row) is
deliberately unmapped: no public Feishu documentation pins its
semantics.

## Rate limits and freshness

Feishu applies per-app and per-user QPS limits; the client's bounded
retry/backoff handles transient 429/5xx envelopes. Scans fetch pages on
demand and stop early under `LIMIT`; completed scans are cached per the
scan cache's usual keying (binding, table, pushed inputs, LIMIT).
