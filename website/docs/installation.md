---
sidebar_position: 2
---

## Installation

### Docker (GHCR)

Pre-built Docker images are published to GitHub Container Registry on every release.

```bash
# Default image
docker pull ghcr.io/skardilabs/skardi/skardi-server:latest

# With ONNX inference support
docker pull ghcr.io/skardilabs/skardi/skardi-server-onnx:latest

# Pull a specific version
docker pull ghcr.io/skardilabs/skardi/skardi-server:0.1.0
docker pull ghcr.io/skardilabs/skardi/skardi-server-onnx:0.1.0
```

### CLI Binary

Download the latest CLI binary for your platform:

```bash
curl -fSL "https://github.com/SkardiLabs/skardi/releases/latest/download/skardi-$(uname -m | sed 's/arm64/aarch64/')-$(uname -s | sed 's/Linux/unknown-linux-gnu/' | sed 's/Darwin/apple-darwin/').tar.gz" | tar xz
sudo mv skardi /usr/local/bin/
```

Or download manually from the [Releases](https://github.com/SkardiLabs/skardi/releases) page. Available targets:

| Platform | Target |
|----------|--------|
| Linux x86_64 | `skardi-x86_64-unknown-linux-gnu.tar.gz` |
| Linux ARM64 | `skardi-aarch64-unknown-linux-gnu.tar.gz` |
| macOS ARM64 (Apple Silicon) | `skardi-aarch64-apple-darwin.tar.gz` |

> **Note:** macOS Intel (x86_64) binaries are not provided. Apple no longer produces Intel-based Macs. You can [build from source](#building-from-source) if needed.

## Building from Source

```bash
# Clone the repository
git clone https://github.com/SkardiLabs/skardi.git
cd skardi

# Build CLI
cargo build --release -p skardi-cli

# Or install CLI globally
cargo install --path crates/cli

# Build server
cargo build --release -p skardi-server

# Build server with ONNX model inference support
cargo build --release -p skardi-server --features onnx
```
