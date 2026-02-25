# AGENTS.md

## Cursor Cloud specific instructions

This is a Rust CLI project ("OmniRust"). The Rust toolchain (rustc, cargo, clippy, rustfmt) is pre-installed.

### Key commands

| Task | Command |
|------|---------|
| Build | `cargo build` |
| Run | `cargo run` |
| Test | `cargo test` |
| Lint | `cargo clippy -- -D warnings` |
| Format check | `cargo fmt -- --check` |
| Format fix | `cargo fmt` |

### Notes

- No external services (databases, Docker, etc.) are required. This is a pure Rust CLI binary.
- The project uses `edition = "2021"` and dual-licensed under MIT/Apache-2.0.
- No pre-commit hooks or CI pipelines are configured yet.
