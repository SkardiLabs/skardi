#!/usr/bin/env bash
# Tests for the crates.io release scripts: scripts/bump_version.sh and
# scripts/publish_crates.sh. Runs offline against copies and a fake index.
#
# The failures these guard against only show up at release time, which is
# where the v0.6.0 publish broke: skardi gained a path dependency on
# skardi-source-pack that carried no version and was not published. CI's
# `cargo package` step catches a dependency with no version; the publish-set
# check below catches a new workspace crate that skardi depends on but that
# the release never publishes.
set -euo pipefail

cd "$(dirname "$0")/.."

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

failures=0
pass() { echo "ok   - $1"; }
fail() {
  echo "FAIL - $1" >&2
  failures=$((failures + 1))
}
# `check <description> <command...>`: pass when the command succeeds.
check() {
  local desc=$1
  shift
  if "$@"; then pass "$desc"; else fail "$desc"; fi
}

# --- bump_version.sh ---------------------------------------------------------

cp Cargo.toml "$tmp/Cargo.toml"
if scripts/bump_version.sh 9.9.9-rc.1 "$tmp/Cargo.toml"; then
  pass "bump succeeds on the real manifest"
else
  fail "bump fails on the real manifest"
fi
check "bump sets workspace.package.version" \
  grep -q '^version = "9.9.9-rc.1"$' "$tmp/Cargo.toml"
check "bump moves the skardi-source-pack pin" \
  grep -q '^skardi-source-pack = .*version = "=9.9.9-rc.1"' "$tmp/Cargo.toml"
changed=$(diff Cargo.toml "$tmp/Cargo.toml" | grep -c '^>' || true)
check "bump changes exactly those two lines (changed: $changed)" \
  test "$changed" -eq 2

# Negative control: if the pin line stops matching (reformatted, renamed), the
# bump must fail rather than leave the pin behind.
grep -v '^skardi-source-pack = ' Cargo.toml >"$tmp/no-pin.toml"
if scripts/bump_version.sh 9.9.9 "$tmp/no-pin.toml" 2>/dev/null; then
  fail "bump succeeds on a manifest without the pin"
else
  pass "bump fails on a manifest without the pin"
fi

# --- the pin matches the workspace version -------------------------------------

workspace_version=$(sed -nE 's/^version = "([^"]*)"$/\1/p' Cargo.toml)
check "skardi-source-pack pin matches workspace version ${workspace_version}" \
  grep -q "^skardi-source-pack = .*version = \"=${workspace_version}\"" Cargo.toml

# --- publish_crates.sh --plan ---------------------------------------------------

metadata=$(cargo metadata --no-deps --format-version 1)
version_of() {
  jq -r --arg name "$1" '.packages[] | select(.name == $name) | .version' <<<"$metadata"
}
pack_version=$(version_of skardi-source-pack)
skardi_version=$(version_of skardi)

# `index_entry <dir> <crate> <version>`: add a version to a fake sparse index.
index_entry() {
  local name=$2 path
  path="$1/${name:0:2}/${name:2:2}/$name"
  mkdir -p "$(dirname "$path")"
  printf '{"name":"%s","vers":"%s"}\n' "$name" "$3" >>"$path"
}

plan() { CRATES_INDEX_DIR=$1 scripts/publish_crates.sh --plan 2>/dev/null; }

expect_plan() {
  local desc=$1 index=$2 want=$3 got
  got=$(plan "$index")
  check "plan: $desc (got '$got', want '$want')" test "$got" = "$want"
}

mkdir -p "$tmp/index-empty"
expect_plan "nothing published yet -> both crates" \
  "$tmp/index-empty" "skardi-source-pack skardi"

index_entry "$tmp/index-older" skardi-source-pack 0.0.1
index_entry "$tmp/index-older" skardi 0.0.1
expect_plan "only older versions published -> both crates" \
  "$tmp/index-older" "skardi-source-pack skardi"

# The retry case: the pack's upload landed, skardi's did not.
index_entry "$tmp/index-pack" skardi-source-pack 0.0.1
index_entry "$tmp/index-pack" skardi-source-pack "$pack_version"
expect_plan "pack already published -> skardi only" \
  "$tmp/index-pack" "skardi"

index_entry "$tmp/index-both" skardi-source-pack "$pack_version"
index_entry "$tmp/index-both" skardi "$skardi_version"
expect_plan "both already published -> nothing" "$tmp/index-both" ""

# --- every published crate's path dependencies are published -------------------

# The full plan against an empty index is the whole publish set.
read -ra published <<<"$(plan "$tmp/index-empty")"
for crate in "${published[@]}"; do
  # Normal and build dependencies only: cargo strips a dev-dependency that has
  # no version, and one that does resolves from crates.io like any other.
  while read -r dep; do
    [[ -z $dep ]] && continue
    if [[ " ${published[*]} " == *" $dep "* ]]; then
      pass "$crate's path dependency $dep is published"
    else
      fail "$crate depends on $dep by path, but publish_crates.sh does not publish it"
    fi
  done < <(jq -r --arg name "$crate" '
    .packages[] | select(.name == $name) | .dependencies[]
    | select(.path != null and .kind != "dev") | .name' <<<"$metadata")
done

if [[ $failures -gt 0 ]]; then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "all release script checks passed"
