# OmniRust

**OmniRust — All your developer tools. One blazingly fast Rust binary.**

[![CI](https://github.com/aCoder2013/OmniRust/actions/workflows/ci.yml/badge.svg)](https://github.com/aCoder2013/OmniRust/actions/workflows/ci.yml)
[![Release](https://github.com/aCoder2013/OmniRust/actions/workflows/release.yml/badge.svg)](https://github.com/aCoder2013/OmniRust/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Rust-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)

## ✨ Why OmniRust?

- **Blazing Fast**: Written entirely in Rust — zero runtime overhead, memory safe, starts in < 10ms
- **All-in-One**: Essential developer tools packed into **a single CLI binary**
- **CLI First**: Clean subcommand design with colorful output
- **Cross-Platform**: Native support for Linux, macOS, and Windows (including musl static builds)
- **DuckDB Inside**: Columnar analytical engine for lightning-fast JSON analysis

## 🚀 Installation

### From GitHub Releases (recommended)

Download the latest pre-built binary for your platform from [Releases](https://github.com/aCoder2013/OmniRust/releases).

| Platform | Target | Download |
|----------|--------|----------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | `omnirust-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64 (static) | `x86_64-unknown-linux-musl` | `omnirust-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | `omnirust-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz` |
| macOS x86_64 | `x86_64-apple-darwin` | `omnirust-vX.Y.Z-x86_64-apple-darwin.tar.gz` |
| macOS Apple Silicon | `aarch64-apple-darwin` | `omnirust-vX.Y.Z-aarch64-apple-darwin.tar.gz` |
| Windows x86_64 | `x86_64-pc-windows-msvc` | `omnirust-vX.Y.Z-x86_64-pc-windows-msvc.zip` |

```bash
# Example: Linux x86_64
curl -LO https://github.com/aCoder2013/OmniRust/releases/latest/download/omnirust-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
tar xzf omnirust-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
sudo mv omnirust /usr/local/bin/

# Verify
omnirust --version
```

### Using Cargo

```bash
cargo install --git https://github.com/aCoder2013/OmniRust.git
```

### Build from source

```bash
git clone https://github.com/aCoder2013/OmniRust.git
cd OmniRust
cargo build --release
# Binary at target/release/omnirust
```

## 📖 Usage

### `json` — JSON Analysis (powered by DuckDB)

Load any JSON file and analyze it with SQL — no setup, no dependencies.

```bash
# Auto-detect schema
omnirust json schema data.json

# Preview first N rows
omnirust json head data.json -n 10

# Run SQL queries (use 'data' as table name)
omnirust json query data.json -s "SELECT city, AVG(salary) FROM data GROUP BY city ORDER BY 2 DESC"

# Column statistics (count, min, max, avg, std, percentiles)
omnirust json stats data.json

# Bar chart — categorical column
omnirust json chart data.json -c department -t bar

# Histogram — numerical column
omnirust json chart data.json -c salary -t hist --bins 10
```

**Example output:**
```
  ✓ Loaded employees.json (30 rows)

  Bar Chart — value counts for 'department'

  Engineering │ ████████████████████████████████████████ 14
    Marketing │ █████████████████ 6
      Product │ ██████████████ 5
       Design │ ██████████████ 5
```

### `ts` — Timestamp Tools

Convert, compare, and inspect timestamps in the terminal.

```bash
# Current timestamp (supports -u s/ms/us/ns)
omnirust ts now
omnirust ts now -u ms

# Timestamp → human-readable date (auto-detects s/ms/us/ns)
omnirust ts to-date 1700000000
omnirust ts to-date 1700000000000        # auto-detects milliseconds

# Date string → timestamp
omnirust ts to-ts "2024-06-15 12:30:00"
omnirust ts to-ts "2024-06-15" -u ms

# Duration between two points (accepts timestamps or date strings)
omnirust ts diff "2024-01-01" "2024-12-31"
omnirust ts diff 1700000000 1700086400
```

**Example output:**
```
  Timestamp  1772007435 (s)
  UTC        2026-02-25 08:17:15.909 UTC
  Local      2026-02-25 08:17:15.909 +00:00
  ISO 8601   2026-02-25T08:17:15.909317735+00:00
```

## 🔧 Development

```bash
cargo build          # Build
cargo test           # Run tests
cargo clippy         # Lint
cargo fmt            # Format
cargo run -- --help  # Run
```

## 🏗️ CI/CD

- **CI**: Every push/PR runs format check, clippy, tests, and multi-platform builds
- **Release**: Push a `v*` tag to automatically build binaries for 6 targets and create a GitHub Release

```bash
# To create a release:
git tag v0.1.0
git push origin v0.1.0
```

## 📄 License

MIT OR Apache-2.0
