# AGENTS.md

## Cursor Cloud specific instructions

This is a Rust CLI project ("OmniRust") with DuckDB-based JSON analysis. The Rust toolchain (rustc, cargo, clippy, rustfmt) is pre-installed.

### Key commands

| Task | Command |
|------|---------|
| Build | `cargo build` |
| Run | `cargo run` |
| Test | `cargo test` |
| Lint | `cargo clippy -- -D warnings` |
| Format check | `cargo fmt -- --check` |
| Format fix | `cargo fmt` |

### Build gotchas

- **C++ compiler**: DuckDB bundled build requires `libstdc++-13-dev` and `CXX=g++`. The default `cc` (clang) cannot find C++ standard library headers without this. Set `export CXX=g++` before building, or add it to `~/.bashrc`.
- **libstdc++ symlink**: A symlink at `/usr/lib/x86_64-linux-gnu/libstdc++.so` -> `/usr/lib/gcc/x86_64-linux-gnu/13/libstdc++.so` is needed for linking. Create with: `sudo ln -sf /usr/lib/gcc/x86_64-linux-gnu/13/libstdc++.so /usr/lib/x86_64-linux-gnu/libstdc++.so`
- **Rust version**: Requires Rust >= 1.84.1. Run `rustup update stable && rustup default stable` if the pre-installed version is older.
- **First build time**: ~5 minutes due to DuckDB C++ compilation from source.

### Example usage

```bash
cargo run -- json schema examples/employees.json
cargo run -- json head examples/employees.json -n 5
cargo run -- json query examples/employees.json -s "SELECT department, AVG(salary) FROM data GROUP BY department"
cargo run -- json stats examples/employees.json
cargo run -- json chart examples/employees.json -c department -t bar
cargo run -- json chart examples/employees.json -c salary -t hist
```

### Notes

- No external services (databases, Docker, etc.) are required. DuckDB runs in-memory.
- The project uses `edition = "2021"` and dual-licensed under MIT/Apache-2.0.
- No pre-commit hooks or CI pipelines are configured yet.
