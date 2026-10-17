# Lark Source Pack

The built-in `lark` source pack exposes a Lark user's own workspace —
chats and their messages and members, tasks, wiki spaces/nodes, and a
docx document's text and block structure — as stable SQL tables through
an [Open Connector gateway](open-connector.md). Lark is Feishu's
international edition, and this pack is `packs/feishu.yaml` mirrored
one-for-one: same tables, same columns, same bindings, with every action
renamed `feishu.<x>` → `lark.<x>`. See
[the Feishu pack doc](open-connector-feishu.md) for the full design
rationale (it applies unchanged), and `packs/lark.yaml` /
`packs/lark.rs` for the pack asset and its tests.

> ⚠️ **Unverified against a live Lark gateway.** Every specific claim in
> the Feishu doc — gateway version, page-size caps, `has_more` semantics,
> required OAuth scopes, the 2026-08-04 / 2026-09-01 reconciliation dates
> — was measured against a real **Feishu** workspace, not Lark. This pack
> carries those facts over on the assumption that Lark's `lark.*` actions
> are wire-identical to Feishu's `feishu.*` actions, which has not been
> independently confirmed. Re-run the same live-verification pass against
> a real Lark gateway before relying on this pack in production, and fold
> the real findings back into `packs/lark.yaml`'s header and this doc.

## Binding

Identical shape to Feishu, with `source_pack: lark` and the `lark.*`
tables:

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
          - name: team
            source_pack: lark
            tables: [chats, tasks, wiki_spaces]
          - name: standup
            source_pack: lark
            resource:
              containerId: oc_a1b2c3d4e5f6
            tables: [messages]
          - name: standup_members
            source_pack: lark
            resource:
              chatId: oc_a1b2c3d4e5f6
            tables: [chat_members]
          - name: handbook
            source_pack: lark
            resource:
              spaceId: "7034502641455497244"
            tables: [wiki_nodes]
          - name: onboarding_doc
            source_pack: lark
            resource:
              documentId: doxcnAbCdEfGhIjKlMnOpQr
            tables: [document_content, document_blocks]
```

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|---|---|---|---|---|
| `chats` | `lark.list_chats` | — | cursor, 100/page | — |
| `messages` | `lark.list_messages` | `containerId` (required) | cursor, 50/page | `create_time >=` → `startTime` (inexact) |
| `chat_members` | `lark.list_chat_members` | `chatId` (required) | cursor, 100/page | — |
| `document_content` | `lark.get_document_content` | `documentId` (required) | single page (one row per document) | — |
| `document_blocks` | `lark.list_document_blocks` | `documentId` (required) | cursor, 500/page | — |
| `tasks` | `lark.list_tasks` | — | cursor, 100/page | — |
| `wiki_spaces` | `lark.list_wiki_spaces` | — | cursor, 50/page | — |
| `wiki_nodes` | `lark.list_wiki_nodes` | `spaceId` (required), `parentNodeToken` (optional) | cursor, 50/page | — |

All design notes, operational findings (OAuth scopes, page-size caps,
the scope-union OAuth URL caveat, the VPN egress-guard trip), and the
upstream `oomol-lab/open-connector` issue links in the
[Feishu pack doc](open-connector-feishu.md) are carried over verbatim as
assumptions about Lark, not as facts measured against Lark. In
particular, the gateway's `lark.*` actions — if and when Open Connector
ships them — may declare a different scope naming scheme than Feishu's
`im:*` / `docx:*` / `task:*` / `wiki:*` scopes; confirm against your own
Lark app before relying on the scope list.

## Coordinate keys

Unlike the data columns below (which keep the provider's own wire names,
e.g. the `tenant_key` column on `chats` and `chat_members`), this OSS
pack declares no pack-level coordinate key named `tenantKey` — Skardi
Cloud's internal `{larkTenantKey, documentId}` / `{larkTenantKey,
chatId}` coordinate scheme is a cloud-side concept, not something
`packs/lark.yaml`'s `resources:` blocks express today (the resources
here are just `containerId` / `chatId` / `documentId` / `spaceId`, same
as Feishu's). If a future revision of this pack introduces a Lark
tenant-scoping resource input, it should be named `larkTenantKey` rather
than reusing Feishu's `tenantKey` spelling, per Skardi Cloud's coordinate
model.
