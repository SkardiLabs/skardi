#!/usr/bin/env bash
# `skardi-source-pack` must stay importable by a syncer.
#
# The crate exists so that three consumers — the engine, cloud's ETL runner and
# cloud's rbac syncer — share one Open Connector client and one paging loop.
# Two of those three cannot depend on the engine, because the engine drags in
# DataFusion and Arrow: a process that mirrors an ACL has no business compiling
# a query planner.
#
# So the boundary IS the reason the crate exists, and it is the kind of thing
# that rots silently — nobody adds `datafusion` on purpose, they add a crate
# that happens to pull it in. A direct dependency is obvious in review; a
# transitive one is invisible, which is why this asks the resolved dependency
# TREE rather than reading `Cargo.toml`.
#
# WHAT THIS DOES NOT CHECK, and where the other half is verified.
#
# Tree purity is one of two properties the crate needs. The other is that its
# manifest DECLARES every feature it uses: inside this workspace cargo unifies
# features across all members, so the pack can call `tokio::time::timeout_at`
# while declaring no `time` feature and compile perfectly here — then fail in
# cloud, which has no such member to borrow it from. That exact bug existed
# and was found by building the pack from a throwaway crate outside the
# workspace:
#
#   a crate whose ONLY dependencies are `skardi-source-pack` and `serde_json`
#   (no tokio, no reqwest), binding an `ActionScan` and a `TransportPolicy`
#
# That is not run here. It needs a cold target directory to mean anything —
# a shared one hides the failure behind cached artifacts — and a cold build of
# reqwest's TLS stack is minutes. Cloud's own build is the standing check, so a
# missing feature surfaces there rather than never. Run the throwaway crate by
# hand when changing what the pack imports.
#
# Run it anywhere:  ./scripts/assert_pack_boundary.sh
set -uo pipefail

PACK=skardi-source-pack

# Each of these, transitively, would take away the crate's portability:
#
#   datafusion, arrow      the query planner and its memory format — the whole
#                          point of not depending on the engine
#   sqlx                   cloud's rbac owns its own database access
#   kube, k8s-openapi      the operator's concern, not a connector's
FORBIDDEN=(datafusion datafusion-common arrow arrow-array arrow-schema sqlx kube k8s-openapi)

# Is `$1` anywhere in the pack's resolved tree?
#
# `cargo tree --invert` prints who depends on a package and exits non-zero when
# it is absent. Two details this wraps, both of which have bitten:
#
#   * the status must be CARGO's. `set -o pipefail` at the top is what
#     guarantees that if anyone ever pipes this — without it, `cargo tree …
#     | tail` reports tail's success and this function answers "present" for
#     everything, which turns every check below into a false alarm. That
#     option is load-bearing, not tidiness.
#   * every caller goes through here, INCLUDING the control below. An earlier
#     version had the control call `cargo tree` on its own line, so breaking
#     the probe used by the loop left the control intact — the script passed
#     while checking nothing, which is precisely the failure the control was
#     added to prevent. Sharing the path is what makes the control mean
#     something.
probe() {
  cargo tree --package "$PACK" --invert "$1" >/dev/null 2>&1
}

# Same, but keeping the output for the error message.
probe_verbose() {
  cargo tree --package "$PACK" --invert "$1" 2>&1
}

# ── The control runs FIRST, and through the same `probe` ────────────────────
# `serde_json` is certainly in this tree. If the probe cannot find it, the
# probe is broken, and every "absent" answer below would be a false pass.
if ! probe serde_json; then
  echo "ERROR: the control probe found no 'serde_json' in $PACK's tree." >&2
  echo "       That dependency certainly exists, so this probe is no longer" >&2
  echo "       asking the question it thinks it is asking, and any pass it" >&2
  echo "       reports below would be meaningless. Fix the probe before" >&2
  echo "       trusting a green run." >&2
  exit 1
fi

fail=0
for dep in "${FORBIDDEN[@]}"; do
  if probe "$dep"; then
    echo "ERROR: $PACK depends on '$dep', which it must not." >&2
    echo >&2
    probe_verbose "$dep" | head -20 >&2
    echo >&2
    echo "  Why this is a failure and not a preference: the ETL runner and the" >&2
    echo "  rbac syncer import this crate precisely to avoid the engine's" >&2
    echo "  dependency weight. A transitive '$dep' means they now carry it," >&2
    echo "  and the crate no longer has a reason to be separate." >&2
    fail=1
  fi
done

[ "$fail" -eq 0 ] || exit 1

echo "OK: $PACK's dependency tree carries none of: ${FORBIDDEN[*]}"
echo "    (control: serde_json was found through the same probe, so it works)"
