# PFS  - PFS File System

A content-addressed filesystem implemented in Rust using FUSE. Files are stored by their SHA-256 hash with metadata persisted using the fjall embedded database.

## Usage

```bash
# Build
cargo build --release

# Run filesystem daemon
cargo run --bin pfsd

# Run tests (single-threaded for FUSE compatibility)
cargo test --test basics -- --test-threads=1
```

The filesystem mounts at `$XDG_DATA_HOME/pfs` (or `$HOME/.local/share/pfs`) with data stored in `$XDG_DATA_HOME/pfs_data`.

## AI disclaimer
I was/am and skeptic, but I started using Claude Code for this and it's really helpful.
Some or most of the code in this repository might be generated using AI.
If that's what it takes for me to actually implement this dream of mine, so be it.
