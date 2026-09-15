# Homebrew formula template for the skardi CLI.
#
# This file is NOT a working formula: the VERSION/SHA256 tokens are filled
# in by the `homebrew` job in .github/workflows/release.yml at release time and the
# rendered result is pushed to SkardiLabs/homebrew-tap (Formula/skardi.rb).
# Users then install with:
#
#   brew install skardilabs/tap/skardi
#
# Every url points at a tarball already attached to the GitHub Release, so the
# formula never rebuilds anything and stays in lockstep with the published
# artifacts. Homebrew derives the formula version from the literal tarball
# urls, so no explicit `version` stanza is needed (brew audit rejects it).
class Skardi < Formula
  desc "CLI for Skardi — federated SQL engine and YAML-driven API server"
  homepage "https://github.com/SkardiLabs/skardi"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/SkardiLabs/skardi/releases/download/v@VERSION@/skardi-aarch64-apple-darwin.tar.gz"
      sha256 "@SHA256_AARCH64_APPLE_DARWIN@"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/SkardiLabs/skardi/releases/download/v@VERSION@/skardi-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "@SHA256_X86_64_UNKNOWN_LINUX_GNU@"
    end
    on_arm do
      url "https://github.com/SkardiLabs/skardi/releases/download/v@VERSION@/skardi-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "@SHA256_AARCH64_UNKNOWN_LINUX_GNU@"
    end
  end

  def install
    bin.install "skardi"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/skardi --version")
  end
end
