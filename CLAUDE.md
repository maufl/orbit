# Claude Code Session Knowledge

## Project Overview
- **PFS**: A content-addressed filesystem implemented in Rust using FUSE
- Files stored by SHA-256 hash with metadata persisted using fjall embedded database
- Main components: `src/lib.rs` (core), `src/bin/pfsd.rs` (daemon), `tests/basics.rs` (integration tests)

## Key Architecture Decisions
- **Pfs struct**: Contains non-optional `Keyspace`, `fs_nodes_partition`, and `directories_partition` fields
- **Initialization**: `Pfs::initialize()` returns `Result<Pfs, Box<dyn std::error::Error>>` - fails early if metadata database can't be opened
- **Persistence**: Uses fjall key-value store for filesystem metadata
- **Content Storage**: Files stored in data directory named by content hash

## Development Practices
- **Formatting**: Always run `rustfmt` on `.rs` files after making changes
- **Testing**: Run integration tests with `cargo test --test basics -- --test-threads=1` (single-threaded for FUSE compatibility)
- **Error Handling**: Database initialization failures should cause early failure, not silent degradation
- **Code Quality**: Prefer helper functions over duplicated logic (e.g., `new_file_node_with_persistence`)

## Important Implementation Details
- **FsNode Creation**: Use `new_file_node_with_persistence()` helper for creating and persisting file nodes
- **Directory Structure**: Metadata stored in `{data_dir}/metadata/`, content files in `{data_dir}/`
- **Mount Points**: Default to `$HOME/Orbit` (automatically created)
- **Persistence**: All FsNodes and Directory structures are automatically persisted to fjall database

## Code Patterns
- Prefer editing existing files over creating new ones
- Use `TodoWrite`/`TodoRead` tools for tracking complex multi-step tasks
- Handle fjall database errors by logging and continuing (for persistence operations)
- Use proper error propagation in initialization functions