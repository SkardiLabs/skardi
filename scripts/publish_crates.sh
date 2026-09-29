#!/usr/bin/env bash
# Publish the workspace's crates.io crates, skipping any whose current version
# is already on crates.io.
#
#   scripts/publish_crates.sh            publish
#   scripts/publish_crates.sh --dry-run  package and verify, upload nothing
#   scripts/publish_crates.sh --plan     print the crates that would publish
#
# Why the skip: a crates.io version is immutable, and a publish can fail
# after an upload has already landed. Cargo can time out waiting for the
# index to list the crate it just uploaded, or the next crate's upload can
# fail. A re-run that tried every crate again would stop at the one already
# up and never reach the rest, so each re-run publishes only what is still
# missing.
#
# The remaining crates go to ONE `cargo publish -p ... -p ...`: cargo orders
# them by dependency, waits for each to be listed before publishing its
# dependents, and in dry-run mode verifies a crate against its locally
# packaged, not-yet-uploaded dependencies.
#
# CRATES_INDEX_DIR, when set, is read in place of the crates.io sparse
# index: a directory laid out the same way. scripts/release_scripts_test.sh
# uses it to test the plan without network access.
set -euo pipefail

cd "$(dirname "$0")/.."

# Every crate published to crates.io. Each path dependency of one of these
# must be listed too; scripts/release_scripts_test.sh enforces it.
CRATES=(skardi-source-pack skardi)

INDEX_URL=https://index.crates.io

mode=publish
case "${1:-}" in
  "") ;;
  --dry-run) mode=dry-run ;;
  --plan) mode=plan ;;
  *)
    echo "usage: $0 [--dry-run|--plan]" >&2
    exit 2
    ;;
esac

metadata=$(cargo metadata --no-deps --format-version 1)

version_of() {
  jq -r --arg name "$1" '.packages[] | select(.name == $name) | .version' <<<"$metadata"
}

# A crate's file in the sparse index: https://doc.rust-lang.org/cargo/reference/registry-index.html#index-files
index_path() {
  local name
  name=$(tr '[:upper:]' '[:lower:]' <<<"$1")
  case ${#name} in
    1) echo "1/$name" ;;
    2) echo "2/$name" ;;
    3) echo "3/${name:0:1}/$name" ;;
    *) echo "${name:0:2}/${name:2:2}/$name" ;;
  esac
}

# Succeeds when crates.io already has version $2 of crate $1. An index that
# cannot be read is an error, not "unpublished": guessing wrong either way
# fails the release, and failing here says why.
is_published() {
  local path entries status tmp
  path=$(index_path "$1")
  if [[ -n "${CRATES_INDEX_DIR:-}" ]]; then
    [[ -f "$CRATES_INDEX_DIR/$path" ]] || return 1
    entries=$(cat "$CRATES_INDEX_DIR/$path")
  else
    tmp=$(mktemp)
    status=$(curl -sS -o "$tmp" -w '%{http_code}' "$INDEX_URL/$path") || status=000
    case $status in
      200) ;;
      404) return 1 ;;
      *)
        echo "::error::could not read the crates.io index for $1 (HTTP $status)" >&2
        exit 1
        ;;
    esac
    entries=$(cat "$tmp")
    rm -f "$tmp"
  fi
  jq -s -e --arg vers "$2" 'any(.[]; .vers == $vers)' <<<"$entries" >/dev/null
}

pending=()
for crate in "${CRATES[@]}"; do
  version=$(version_of "$crate")
  if [[ -z $version ]]; then
    echo "::error::$crate is not a workspace package" >&2
    exit 1
  fi
  if is_published "$crate" "$version"; then
    echo "$crate $version is already on crates.io, skipping" >&2
  else
    pending+=("$crate")
  fi
done

if [[ $mode == plan ]]; then
  if [[ ${#pending[@]} -gt 0 ]]; then
    echo "${pending[*]}"
  fi
  exit 0
fi

if [[ ${#pending[@]} -eq 0 ]]; then
  echo "Nothing to publish: every crate's version is already on crates.io." >&2
  exit 0
fi

args=()
for crate in "${pending[@]}"; do
  args+=(-p "$crate")
done
if [[ $mode == dry-run ]]; then
  args+=(--dry-run)
fi

set -x
cargo publish "${args[@]}"
