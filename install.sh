#!/usr/bin/env bash
# Install the skardi CLI, then set up the AI coding agents on this machine
# to use Skardi: copy the Agent Skills into each agent's skills directory and
# register Skardi's MCP server in each agent's config.
#
#   curl -fsSL https://raw.githubusercontent.com/SkardiLabs/skardi/main/install.sh | bash
#
# Re-run with --agents-only to refresh the skills or to set up an agent you
# installed later; it never reinstalls the CLI. Every step is idempotent:
# an MCP entry named `skardi` that already exists is left alone.
#
# Options:
#   --yes             answer yes to every prompt (MCP is still skipped unless
#                     --mcp is given, since there is no safe default target)
#   --agents-only     skip installing the CLI
#   --no-agents       install the CLI only
#   --mcp MODE        cloud | local | none
#   --prefix DIR      where the skardi binary goes (default /usr/local/bin)
#   --with-unreleased also install skills that need Skardi main (a main build
#                     reports the last release's version, so it cannot be told
#                     apart from that release automatically)
#
# Environment:
#   SKARDI_INSTALL_NO_AGENTS=1   same as --no-agents
#   SKARDI_SKILLS_REF            skardi-skills branch or tag (default main)
#   SKARDI_SKILLS_TARBALL        use a local skardi-skills tarball instead of
#                                downloading one (used by the tests)
#   SKARDI_CLOUD_MCP_URL         Skardi Cloud MCP endpoint
#
# Run it as yourself, not with sudo: skills and agent configs belong in your
# home directory. The only step that may ask for sudo is moving the binary
# into a --prefix you cannot write to.

set -euo pipefail

REPO="SkardiLabs/skardi"
SKILLS_REPO="SkardiLabs/skardi-skills"
SKILLS_REF="${SKARDI_SKILLS_REF:-main}"
CLOUD_MCP_URL="${SKARDI_CLOUD_MCP_URL:-https://gateway.skardi.ai/mcp}"
PREFIX="/usr/local/bin"
ASSUME_YES=0
INSTALL_CLI=1
SETUP_AGENTS=1
MCP_MODE=""
WITH_UNRELEASED=0

[ "${SKARDI_INSTALL_NO_AGENTS:-}" = "1" ] && SETUP_AGENTS=0

usage() {
  cat <<'USAGE'
Usage: install.sh [--yes] [--agents-only | --no-agents] [--mcp cloud|local|none]
                  [--prefix DIR] [--with-unreleased]

Installs the skardi CLI, then offers to set up Claude Code, Codex and Cursor:
the Skardi skills in each agent's skills directory, and Skardi's MCP server
in each agent's config. --agents-only redoes the agent setup without
reinstalling the CLI; use it to update the skills or add a new agent.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --yes|-y) ASSUME_YES=1 ;;
    --agents-only) INSTALL_CLI=0 ;;
    --no-agents) SETUP_AGENTS=0 ;;
    --mcp) MCP_MODE="${2:-}"; shift ;;
    --mcp=*) MCP_MODE="${1#--mcp=}" ;;
    --prefix) PREFIX="${2:-}"; shift ;;
    --prefix=*) PREFIX="${1#--prefix=}" ;;
    --with-unreleased) WITH_UNRELEASED=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
  esac
  shift
done

case "$MCP_MODE" in
  ""|cloud|local|none) ;;
  *) echo "--mcp must be cloud, local or none, got '$MCP_MODE'" >&2; exit 2 ;;
esac

say()  { printf '%s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

if [ "$(id -u)" = "0" ] && [ -n "${SUDO_USER:-}" ] && [ "$SETUP_AGENTS" = "1" ]; then
  die "run this as yourself, not with sudo: skills and agent configs would land in root's home. Drop the sudo; the script asks for it only to move the binary."
fi

# Prompts read the terminal, not stdin, so `curl ... | bash` can still ask.
# With no terminal (CI, a Dockerfile) and no --yes, every answer is no:
# nothing outside the CLI install happens unless someone agreed to it.
ask() {
  local question="$1" default="$2" reply=""
  [ "$ASSUME_YES" = "1" ] && return 0
  { [ -r /dev/tty ] && { : </dev/tty; } 2>/dev/null; } || return 1
  printf '%s ' "$question" >/dev/tty
  read -r reply </dev/tty || reply=""
  [ -z "$reply" ] && reply="$default"
  case "$reply" in y|Y|yes|YES) return 0 ;; *) return 1 ;; esac
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

install_cli() {
  local os arch target url
  os="$(uname -s)"; arch="$(uname -m)"
  case "$arch" in arm64) arch="aarch64" ;; esac
  case "$os" in
    Linux)  target="${arch}-unknown-linux-gnu" ;;
    Darwin) target="${arch}-apple-darwin" ;;
    *) die "no pre-built skardi for $os; build from source: https://github.com/$REPO#install" ;;
  esac
  case "$target" in
    x86_64-unknown-linux-gnu|aarch64-unknown-linux-gnu|aarch64-apple-darwin) ;;
    *) die "no pre-built skardi for $target (Intel Macs build from source): https://github.com/$REPO#install" ;;
  esac

  url="https://github.com/$REPO/releases/latest/download/skardi-$target.tar.gz"
  say "Downloading skardi for $target"
  curl -fsSL "$url" -o "$TMP/skardi.tar.gz" || die "download failed: $url"
  if curl -fsSL "$url.sha256" -o "$TMP/skardi.sha256" 2>/dev/null; then
    local want got
    want="$(awk '{print $1}' "$TMP/skardi.sha256")"
    if command -v shasum >/dev/null 2>&1; then
      got="$(shasum -a 256 "$TMP/skardi.tar.gz" | awk '{print $1}')"
    else
      got="$(sha256sum "$TMP/skardi.tar.gz" | awk '{print $1}')"
    fi
    [ "$want" = "$got" ] || die "checksum mismatch for $url"
  fi
  tar -xzf "$TMP/skardi.tar.gz" -C "$TMP"
  [ -f "$TMP/skardi" ] || die "the archive has no skardi binary"
  chmod +x "$TMP/skardi"

  mkdir -p "$PREFIX" 2>/dev/null || true
  if [ -w "$PREFIX" ]; then
    mv "$TMP/skardi" "$PREFIX/skardi"
  else
    say "Moving skardi into $PREFIX needs sudo"
    sudo mv "$TMP/skardi" "$PREFIX/skardi"
  fi
  say "Installed $PREFIX/skardi"
  case ":$PATH:" in *":$PREFIX:"*) ;; *) warn "$PREFIX is not on your PATH" ;; esac
}

# ---------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------

# An agent counts as installed when its config directory exists. That is
# what each agent creates on first run, and it is present even when the
# agent's CLI is not on PATH (a desktop-only install), which is why the MCP
# steps below write config files instead of calling the agents' CLIs.
CLAUDE_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
# User-scoped MCP servers live in .claude.json: in the home directory by
# default, inside CLAUDE_CONFIG_DIR when that is set.
CLAUDE_JSON="${CLAUDE_CONFIG_DIR:+$CLAUDE_CONFIG_DIR/.claude.json}"
CLAUDE_JSON="${CLAUDE_JSON:-$HOME/.claude.json}"
CODEX_DIR="${CODEX_HOME:-$HOME/.codex}"
CURSOR_DIR="$HOME/.cursor"

detect_agents() {
  AGENTS=""
  [ -d "$CLAUDE_DIR" ] && AGENTS="$AGENTS claude"
  [ -d "$CODEX_DIR" ]  && AGENTS="$AGENTS codex"
  [ -d "$CURSOR_DIR" ] && AGENTS="$AGENTS cursor"
  AGENTS="${AGENTS# }"
}

agent_label() {
  case "$1" in claude) echo "Claude Code" ;; codex) echo "Codex" ;; cursor) echo "Cursor" ;; esac
}

fetch_skills() {
  local tarball="$TMP/skills.tar.gz"
  if [ -n "${SKARDI_SKILLS_TARBALL:-}" ]; then
    cp "$SKARDI_SKILLS_TARBALL" "$tarball"
  else
    curl -fsSL "https://codeload.github.com/$SKILLS_REPO/tar.gz/$SKILLS_REF" -o "$tarball" \
      || die "could not download $SKILLS_REPO@$SKILLS_REF"
  fi
  mkdir -p "$TMP/skills-src"
  tar -xzf "$tarball" -C "$TMP/skills-src"
  SKILLS_DIR="$(find "$TMP/skills-src" -mindepth 2 -maxdepth 2 -type d -name skills | head -n 1)"
  [ -n "$SKILLS_DIR" ] || die "$SKILLS_REPO@$SKILLS_REF has no skills/ directory"
  pick_skills
}

# 0 when version $1 >= $2, both x.y.z.
version_ge() {
  local a1 a2 a3 b1 b2 b3
  IFS=. read -r a1 a2 a3 <<<"$1"; IFS=. read -r b1 b2 b3 <<<"$2"
  a1=${a1:-0} a2=${a2:-0} a3=${a3%%[!0-9]*} b1=${b1:-0} b2=${b2:-0} b3=${b3%%[!0-9]*}
  a3=${a3:-0} b3=${b3:-0}
  [ "$a1" -ne "$b1" ] && { [ "$a1" -gt "$b1" ]; return; }
  [ "$a2" -ne "$b2" ] && { [ "$a2" -gt "$b2" ]; return; }
  [ "$a3" -ge "$b3" ]
}

# Each SKILL.md states the oldest Skardi it runs on, as
# metadata.skardi-min-version: a release number, or "main" while no release
# has what it needs. Skills the installed CLI is too old for are left out,
# so the agent is never taught a capability the user's Skardi lacks. With no
# skardi on PATH the version is unknown and only "main" skills are held back.
pick_skills() {
  local bin version skill name min skipped=""
  bin="$(skardi_bin)"
  version=""
  [ -n "$bin" ] && version="$("$bin" --version 2>/dev/null | awk '{print $2}')" || true
  PICKED=""
  for skill in "$SKILLS_DIR"/*/; do
    [ -f "$skill/SKILL.md" ] || continue
    name="$(basename "$skill")"
    min="$(awk 'NR==1 && /^---/ {f=1; next} f && /^---/ {exit} f && /^  skardi-min-version:/ {gsub(/"/, "", $2); print $2}' "$skill/SKILL.md")"
    if [ "$min" = "main" ] && [ "$WITH_UNRELEASED" != "1" ]; then
      skipped="$skipped $name"; continue
    fi
    if [ -n "$min" ] && [ "$min" != "main" ] && [ -n "$version" ] && ! version_ge "$version" "$min"; then
      skipped="$skipped $name"; continue
    fi
    PICKED="$PICKED $name"
  done
  PICKED="${PICKED# }"
  if [ -n "$skipped" ]; then
    say "Not installed, they need a newer Skardi than ${version:-the latest release}:${skipped}"
    say "  (on a build of Skardi main, re-run with --agents-only --with-unreleased)"
  fi
}

# Copy every skill into DEST. A directory we did not install (no marker)
# is someone else's and is left untouched.
copy_skills() {
  local dest="$1" skill name
  mkdir -p "$dest"
  for name in $PICKED; do
    skill="$SKILLS_DIR/$name"
    if [ -d "$dest/$name" ] && [ ! -f "$dest/$name/.skardi-install" ]; then
      warn "$dest/$name exists and was not installed by this script; left as is"
      continue
    fi
    rm -rf "$dest/$name"
    cp -R "$skill" "$dest/$name"
    printf 'source=%s@%s\n' "$SKILLS_REPO" "$SKILLS_REF" >"$dest/$name/.skardi-install"
  done
}

claude_has_plugin() {
  local f="$CLAUDE_DIR/plugins/installed_plugins.json"
  [ -f "$f" ] && grep -q '"skardi@skardi-skills"' "$f"
}

install_skills() {
  local agent shared_done=0
  for agent in $SELECTED; do
    case "$agent" in
      claude)
        if claude_has_plugin; then
          say "  Claude Code: the skardi plugin is already installed; skills left to it"
        else
          copy_skills "$CLAUDE_DIR/skills"
          say "  Claude Code: skills in $CLAUDE_DIR/skills"
        fi ;;
      codex|cursor)
        # Codex and Cursor both read the cross-tool ~/.agents/skills, so one
        # copy serves both.
        if [ "$shared_done" = "0" ]; then
          copy_skills "$HOME/.agents/skills"
          shared_done=1
        fi
        say "  $(agent_label "$agent"): skills in $HOME/.agents/skills" ;;
    esac
  done
}

need_python() {
  command -v python3 >/dev/null 2>&1 && return 0
  warn "python3 not found; $1 was not configured. Add the skardi MCP server by hand: https://github.com/$REPO/blob/main/docs/mcp.md"
  return 1
}

# Add mcpServers.skardi to a JSON config unless it is already there.
add_json_mcp() {
  local file="$1" entry="$2"
  need_python "$file" || return 0
  python3 - "$file" "$entry" <<'PY'
import json, os, sys
path, entry = sys.argv[1], json.loads(sys.argv[2])
data = {}
if os.path.exists(path) and os.path.getsize(path) > 0:
    with open(path) as f:
        data = json.load(f)
servers = data.setdefault("mcpServers", {})
if "skardi" in servers:
    print("    already has a skardi MCP server; left as is")
    sys.exit(0)
servers["skardi"] = entry
os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
tmp = path + ".skardi-tmp"
with open(tmp, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")
if os.path.exists(path):
    os.chmod(tmp, os.stat(path).st_mode & 0o777)
else:
    os.chmod(tmp, 0o600)
os.replace(tmp, path)
PY
}

add_codex_mcp() {
  local file="$CODEX_DIR/config.toml"
  if [ -f "$file" ] && grep -q '^\[mcp_servers\.skardi\]' "$file"; then
    say "    already has a skardi MCP server; left as is"
    return 0
  fi
  mkdir -p "$CODEX_DIR"
  {
    printf '\n[mcp_servers.skardi]\n'
    if [ "$MCP_MODE" = "cloud" ]; then
      printf 'url = "%s"\n' "$CLOUD_MCP_URL"
    else
      printf 'command = "skardi"\nargs = ["mcp"]\n'
    fi
  } >>"$file"
}

# User scope, so every project sees it. A running Claude Code rewrites
# .claude.json often, so its own CLI is used when it is on PATH; editing the
# file directly is the fallback for a desktop-only install.
add_claude_mcp() {
  if command -v claude >/dev/null 2>&1; then
    if claude mcp get skardi >/dev/null 2>&1; then
      say "    already has a skardi MCP server; left as is"
    elif [ "$MCP_MODE" = "cloud" ]; then
      claude mcp add --scope user --transport http skardi "$CLOUD_MCP_URL" >/dev/null
    else
      claude mcp add --scope user skardi -- skardi mcp >/dev/null
    fi
  elif [ "$MCP_MODE" = "cloud" ]; then
    add_json_mcp "$CLAUDE_JSON" "$1"
  else
    add_json_mcp "$CLAUDE_JSON" '{"type":"stdio","command":"skardi","args":["mcp"],"env":{}}'
  fi
}

skardi_bin() {
  if [ -x "$PREFIX/skardi" ]; then echo "$PREFIX/skardi"; else command -v skardi || true; fi
}

choose_mcp_mode() {
  [ -n "$MCP_MODE" ] && return 0
  if [ "$ASSUME_YES" = "1" ]; then MCP_MODE="none"; return 0; fi
  if ask "Connect your agents to Skardi Cloud over MCP? [y/N]" "n"; then
    MCP_MODE="cloud"
  elif ask "Connect them to a local skardi-server instead? [y/N]" "n"; then
    MCP_MODE="local"
  else
    MCP_MODE="none"
  fi
}

setup_mcp() {
  choose_mcp_mode
  [ "$MCP_MODE" = "none" ] && { say "MCP: skipped"; return 0; }

  if [ "$MCP_MODE" = "local" ]; then
    local bin; bin="$(skardi_bin)"
    if [ -z "$bin" ] || ! "$bin" mcp --help >/dev/null 2>&1; then
      warn "this skardi has no 'mcp' command (it is on main, not in the latest release yet); local MCP skipped"
      return 0
    fi
  fi

  local http_entry stdio_entry agent
  http_entry="{\"type\":\"http\",\"url\":\"$CLOUD_MCP_URL\"}"
  stdio_entry='{"command":"skardi","args":["mcp"]}'
  say "MCP ($MCP_MODE):"
  for agent in $SELECTED; do
    say "  $(agent_label "$agent")"
    case "$agent" in
      claude) add_claude_mcp "$http_entry" "$stdio_entry" ;;
      codex) add_codex_mcp ;;
      cursor)
        if [ "$MCP_MODE" = "cloud" ]; then
          add_json_mcp "$CURSOR_DIR/mcp.json" "{\"url\":\"$CLOUD_MCP_URL\"}"
        else
          add_json_mcp "$CURSOR_DIR/mcp.json" "$stdio_entry"
        fi ;;
    esac
  done
  if [ "$MCP_MODE" = "cloud" ]; then
    say ""
    say "Each agent asks you to sign in to Skardi Cloud in the browser the first time it connects:"
    case " $SELECTED " in *" claude "*) say "  Claude Code: run  claude mcp login skardi  (or /mcp inside a session)" ;; esac
    case " $SELECTED " in *" codex "*)  say "  Codex:       run  codex mcp login skardi" ;; esac
    case " $SELECTED " in *" cursor "*) say "  Cursor:      open Settings > MCP and sign in to skardi" ;; esac
  else
    say ""
    say "skardi mcp talks to the skardi-server your config points at (default http://localhost:8080)."
  fi
}

setup_agents() {
  detect_agents
  if [ -z "$AGENTS" ]; then
    say "No supported AI coding agent found (Claude Code, Codex, Cursor). Re-run with --agents-only after installing one."
    return 0
  fi
  local labels="" a
  for a in $AGENTS; do labels="$labels, $(agent_label "$a")"; done
  labels="${labels#, }"
  if ! ask "Set up Skardi skills and MCP for $labels? [Y/n]" "y"; then
    say "Skipped. Run again with --agents-only whenever you want it."
    return 0
  fi
  SELECTED="$AGENTS"
  fetch_skills
  say "Skills ($SKILLS_REPO@$SKILLS_REF):"
  install_skills
  setup_mcp
}

[ "$INSTALL_CLI" = "1" ] && install_cli
[ "$SETUP_AGENTS" = "1" ] && setup_agents
exit 0
