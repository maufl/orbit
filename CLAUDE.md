# Claude Code Session Knowledge

## Project Overview
- **PFS**: A distributed content-addressed filesystem implemented in Rust using FUSE
- Files stored by SHA-256 hash with metadata persisted using fjall embedded database
- Network communication using iroh for peer-to-peer connections
- Main components: `src/lib.rs` (core), `src/config.rs` (configuration), `src/bin/pfsd.rs` (daemon), `tests/basics.rs` (integration tests)

## Key Architecture Decisions
- **Pfs struct**: Uses `PfsRuntimeData` wrapped in `Arc<parking_lot::RwLock<_>>` for thread-safe runtime state
- **Runtime Data**: Contains `directories`, `inodes`, and `open_files` - consolidated from separate Arc<RwLock<_>> fields
- **Concurrency**: Uses `parking_lot::RwLock` instead of `std::sync::RwLock` for better performance
- **Initialization**: `Pfs::initialize()` returns `Result<Pfs, Box<dyn std::error::Error>>` - fails early if metadata database can't be opened
- **Persistence**: Uses fjall key-value store for filesystem metadata
- **Content Storage**: Files stored in data directory named by content hash
- **Configuration**: Centralized in `src/config.rs` module with TOML serialization and XDG compliance

## Development Practices
- **Formatting**: Always run `rustfmt` on `.rs` files after making changes
- **Testing**: Run integration tests with `cargo test --test basics -- --test-threads=1` (single-threaded for FUSE compatibility)
- **Build Verification**: Run `cargo check` for quick compilation checks, `cargo build` for full builds
- **Persistence Testing**: `test_persistence_infrastructure` verifies full file/directory restoration across filesystem restarts
- **Error Handling**: Use `anyhow::Error` for better error handling, database initialization failures cause early failure
- **Code Quality**: Prefer helper functions over duplicated logic, extract common patterns into reusable methods

## Important Implementation Details
- **FsNode Methods**: `is_directory()` method checks if node is a directory using pattern matching
- **FsNode Creation**: Use `new_file_node_with_persistence()` helper for creating and persisting file nodes
- **Directory Structure**: Metadata stored in `{data_dir}/metadata/`, content files in `{data_dir}/`
- **Configuration**: 
  - Default mount point: `$HOME/Orbit`
  - Default data directory: `$XDG_DATA_HOME/pfs_data` (falls back to `$HOME/.local/share/pfs_data`)
  - Config file: `$XDG_CONFIG_HOME/pfsd.toml` (falls back to `$HOME/.config/pfsd.toml`)
  - Automatic private key generation and persistence
- **Persistence**: All FsNodes and Directory structures automatically persisted to fjall database
- **Root Hash Storage**: Root FsNodeHash stored in KV store under special key `__ROOT_HASH__`
- **Metadata Restoration**: Full recursive restoration of filesystem tree from KV store on startup
- **Error Handling**: `load_fs_node()` and `load_directory()` return `Result<T, anyhow::Error>`
- **Hash Calculation**: `persist_fs_node()` and `persist_directory()` calculate hashes internally
- **Directory Updates**: `update_directory_recursive()` method compares old/new directory structures

## Code Patterns
- **File Management**: Prefer editing existing files over creating new ones
- **Task Tracking**: Use `TodoWrite`/`TodoRead` tools for tracking complex multi-step tasks
- **Error Handling**: Handle fjall database errors by logging and continuing (for persistence operations)
- **Function Design**: Use proper error propagation in initialization functions
- **Code Reuse**: Eliminate code duplication by extracting common patterns into helper functions
- **Data Handling**: Mutate loaded data structures instead of creating copies when possible
- **Thread Safety**: Use `parking_lot::RwLock` for better performance than `std::sync::RwLock`
- **Module Organization**: Keep configuration logic in library modules, CLI parsing in binaries

## Dependencies
- **Core**: `fjall` (embedded database), `fuser` (FUSE), `parking_lot` (better locks)
- **Serialization**: `serde`, `ciborium` (CBOR), `toml`
- **Networking**: `iroh` (peer-to-peer), `tokio` (async runtime)
- **Utilities**: `anyhow` (error handling), `clap` (CLI), `hex`, `base64`, `chrono`
