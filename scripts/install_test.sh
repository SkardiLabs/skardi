#!/usr/bin/env bash
# Tests for install.sh's agent setup. Each case runs the script in a fresh
# fake HOME with a local skills tarball, so nothing is downloaded and no real
# agent config is touched. PATH is reduced to the system directories so a
# developer's own `claude` or `skardi` cannot leak into a case.
#
#   bash scripts/install_test.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL="$ROOT/install.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

FAILS=0
pass() { printf 'ok   %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1"; FAILS=$((FAILS + 1)); }
check() { if eval "$2"; then pass "$1"; else fail "$1"; fi; }
# GNU stat first: BSD stat has no -c, while GNU stat -f means something else.
mode() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }

# A skills tarball shaped like codeload's: one top-level directory, skills/ inside.
# alpha runs on 0.5.0, beta needs main, gamma states nothing.
S="$WORK/src/skardi-skills-main/skills"
mkdir -p "$S/alpha" "$S/beta" "$S/gamma"
printf -- '---\nname: alpha\nmetadata:\n  skardi-min-version: "0.5.0"\n---\nbody\n' >"$S/alpha/SKILL.md"
printf -- '---\nname: beta\nmetadata:\n  skardi-min-version: "main"\n---\nbody\n' >"$S/beta/SKILL.md"
printf -- '---\nname: gamma\n---\nbody\n' >"$S/gamma/SKILL.md"
tar -czf "$WORK/skills.tar.gz" -C "$WORK/src" skardi-skills-main

new_home() {
  H="$WORK/home-$1"
  rm -rf "$H"; mkdir -p "$H"
  shift
  for d in "$@"; do mkdir -p "$H/$d"; done
}

run() {
  env -i HOME="$H" PATH="${TEST_PATH:-/usr/bin:/bin}" SKARDI_SKILLS_TARBALL="$WORK/skills.tar.gz" \
    bash "$INSTALL" --agents-only "$@" >"$H/out.log" 2>&1
}

# 1. Cloud MCP for all three agents, from config directories alone.
new_home all .claude .codex .cursor
run --yes --mcp cloud
check "claude skills copied"          '[ -f "$H/.claude/skills/alpha/SKILL.md" ] && [ -f "$H/.claude/skills/gamma/SKILL.md" ]'
check "main-only skill held back"     '[ ! -e "$H/.claude/skills/beta" ] && grep -q "Not installed.* beta" "$H/out.log"'
check "shared skills copied once"     '[ -f "$H/.agents/skills/alpha/SKILL.md" ]'
check "install marker written"        '[ -f "$H/.claude/skills/alpha/.skardi-install" ]'
check "claude MCP is URL-only"        'python3 -c "import json;e=json.load(open(\"$H/.claude.json\"))[\"mcpServers\"][\"skardi\"];assert e==dict(type=\"http\",url=\"https://gateway.skardi.ai/mcp\")"'
check "cursor MCP is URL-only"        'python3 -c "import json;e=json.load(open(\"$H/.cursor/mcp.json\"))[\"mcpServers\"][\"skardi\"];assert e==dict(url=\"https://gateway.skardi.ai/mcp\")"'
check "codex MCP written"             'grep -q "^url = \"https://gateway.skardi.ai/mcp\"" "$H/.codex/config.toml"'
check "new config file is private"    '[ "$(mode "$H/.cursor/mcp.json")" = "600" ]'
check "sign-in hint printed"          'grep -q "codex mcp login skardi" "$H/out.log"'

# 2. Re-running changes nothing that is already there.
run --yes --mcp cloud
check "codex entry not duplicated"    '[ "$(grep -c "^\[mcp_servers.skardi\]" "$H/.codex/config.toml")" = "1" ]'
check "rerun reports existing entry"  'grep -q "already has a skardi MCP server" "$H/out.log"'

# 3. Existing configs are merged into, not replaced.
new_home merge .cursor
echo '{"mcpServers":{"other":{"url":"https://example.com/mcp"}},"keep":1}' >"$H/.cursor/mcp.json"
chmod 644 "$H/.cursor/mcp.json"
run --yes --mcp cloud
check "other servers kept"            'python3 -c "import json;d=json.load(open(\"$H/.cursor/mcp.json\"));assert \"other\" in d[\"mcpServers\"] and d[\"keep\"]==1 and \"skardi\" in d[\"mcpServers\"]"'
check "file mode kept"                '[ "$(mode "$H/.cursor/mcp.json")" = "644" ]'
check "only detected agents touched"  '[ ! -e "$H/.claude" ] && [ ! -e "$H/.codex" ]'

# 4. A skill directory someone else put there is left alone.
new_home foreign .claude
mkdir -p "$H/.claude/skills/alpha"; echo "mine" >"$H/.claude/skills/alpha/SKILL.md"
run --yes --mcp none
check "foreign skill untouched"       'grep -q mine "$H/.claude/skills/alpha/SKILL.md"'
check "other skills still installed"  '[ -f "$H/.claude/skills/gamma/SKILL.md" ]'
check "MCP none writes no config"     '[ ! -e "$H/.claude.json" ]'

# 5. The Claude Code plugin already carries the skills.
new_home plugin .claude/plugins
echo '{"plugins":{"skardi@skardi-skills":[{}]}}' >"$H/.claude/plugins/installed_plugins.json"
run --yes --mcp none
check "no copy next to the plugin"    '[ ! -e "$H/.claude/skills" ]'

# 6. --yes without --mcp does not pick a target on the user's behalf.
new_home noflag .cursor
run --yes
check "MCP skipped without --mcp"     '[ ! -e "$H/.cursor/mcp.json" ] && grep -q "MCP: skipped" "$H/out.log"'

# 7. No terminal and no --yes: nothing happens beyond the CLI.
if { : </dev/tty; } 2>/dev/null; then
  pass "no-terminal case skipped (this shell has a terminal)"
else
  new_home notty .claude
  run --mcp cloud
  check "no terminal means no changes" '[ ! -e "$H/.claude/skills" ] && [ ! -e "$H/.claude.json" ]'
fi

# 8. Local MCP needs a skardi that has the mcp command.
new_home local .cursor
mkdir -p "$H/bin"
printf '#!/bin/sh\nexit 2\n' >"$H/bin/skardi"; chmod +x "$H/bin/skardi"
TEST_PATH="$H/bin:/usr/bin:/bin" run --yes --mcp local
check "local MCP skipped on old CLI"  '[ ! -e "$H/.cursor/mcp.json" ] && grep -q "no .mcp. command" "$H/out.log"'
printf '#!/bin/sh\nexit 0\n' >"$H/bin/skardi"
TEST_PATH="$H/bin:/usr/bin:/bin" run --yes --mcp local
check "local MCP runs skardi mcp"     'python3 -c "import json;e=json.load(open(\"$H/.cursor/mcp.json\"))[\"mcpServers\"][\"skardi\"];assert e==dict(command=\"skardi\",args=[\"mcp\"])"'

# 9. Versions: --with-unreleased, and a CLI older than a skill needs.
new_home unreleased .cursor
run --yes --with-unreleased
check "--with-unreleased installs main-only" '[ -f "$H/.agents/skills/beta/SKILL.md" ]'
new_home oldcli .cursor
mkdir -p "$H/bin"
printf '#!/bin/sh\n[ "$1" = "--version" ] && echo "skardi 0.4.2"\nexit 0\n' >"$H/bin/skardi"; chmod +x "$H/bin/skardi"
TEST_PATH="$H/bin:/usr/bin:/bin" run --yes
check "too-old CLI skips 0.5.0 skill"  '[ ! -e "$H/.agents/skills/alpha" ] && [ -f "$H/.agents/skills/gamma/SKILL.md" ]'
printf '#!/bin/sh\n[ "$1" = "--version" ] && echo "skardi 0.10.0"\nexit 0\n' >"$H/bin/skardi"
TEST_PATH="$H/bin:/usr/bin:/bin" run --yes
check "0.10.0 counts as newer than 0.5.0" '[ -f "$H/.agents/skills/alpha/SKILL.md" ]'

# 10. No agent installed.
new_home none
run --yes
check "no agent is not an error"      'grep -q "No supported AI coding agent found" "$H/out.log"'

# 11. Bad input fails fast.
new_home bad
if run --mcp nowhere; then fail "bad --mcp rejected"; else pass "bad --mcp rejected"; fi

if [ "$FAILS" -gt 0 ]; then
  printf '\n%d failed\n' "$FAILS"; exit 1
fi
printf '\nall passed\n'
