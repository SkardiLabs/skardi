#!/usr/bin/env bash
# Set the workspace version for a release: `workspace.package.version` and the
# exact pin `skardi` carries on `skardi-source-pack`.
#
#   scripts/bump_version.sh <version> [path/to/Cargo.toml]
#
# The two crates are published to crates.io together, so skardi's dependency
# on the pack needs a version as well as a path, and it is pinned exactly
# (`=X.Y.Z`) because the crates release in lockstep. Rewriting only the
# workspace version leaves that pin behind, and the next `cargo` invocation
# fails: the path crate's new version no longer satisfies the old pin.
#
# Both rewrites are verified afterwards, so a reformatted manifest line fails
# the bump here rather than surfacing later as a broken release branch.
# scripts/release_scripts_test.sh exercises this against a copy of the real
# manifest.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <version> [Cargo.toml]" >&2
  exit 2
fi

version=$1
manifest=${2:-Cargo.toml}

# `-i.bak` then rm: the one in-place form GNU and BSD sed both accept.
sed -E -i.bak \
  -e "s/^version = \"[^\"]*\"/version = \"${version}\"/" \
  -e "s/^(skardi-source-pack = \{.*version = \"=)[^\"]*\"/\1${version}\"/" \
  "$manifest"
rm -f "${manifest}.bak"

fail() {
  echo "::error::$1" >&2
  exit 1
}

grep -q "^version = \"${version}\"$" "$manifest" \
  || fail "failed to set workspace.package.version in $manifest"
grep -q "^skardi-source-pack = .*version = \"=${version}\"" "$manifest" \
  || fail "failed to bump the skardi-source-pack pin in $manifest"
