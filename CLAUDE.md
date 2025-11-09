# Claude Code Session Knowledge
**Important:** Any time you make changes to the code and you are done making additional changes, run `cargo check` to see whether there are any errors. If no errors are reported run the tests to see whether they still succeed!

## Project Overview
- **Orbit**: A distributed content-addressed filesystem implemented in Rust using FUSE
- Files stored by SHA-256 hash with metadata persisted using fjall embedded database
- Network communication using iroh for peer-to-peer connections
- Main components: `orbit/src/lib.rs` (core), `orbit/src/config.rs` (configuration), `orbit/src/bin/orbitd.rs` (daemon), `orbit/tests/basics.rs` (integration tests)

## Key Architecture Decisions
- **OrbitFs struct**: Uses `OrbitFsRuntimeData` wrapped in `Arc<parking_lot::RwLock<_>>` for thread-safe runtime state
- **Runtime Data**: Contains `directories`, `inodes`, and `open_files` - consolidated from separate Arc<RwLock<_>> fields
- **Concurrency**: Uses `parking_lot::RwLock` instead of `std::sync::RwLock` for better performance
- **Initialization**: `OrbitFs::initialize()` returns `Result<OrbitFs, Box<dyn std::error::Error>>` - fails early if metadata database can't be opened
- **Persistence**: Uses fjall key-value store for filesystem metadata
- **Content Storage**: Files stored in data directory named by content hash
- **Configuration**: Centralized in `orbit/src/config.rs` module with TOML serialization and XDG compliance

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
  - Default data directory: `$XDG_DATA_HOME/orbit_data` (falls back to `$HOME/.local/share/orbit_data`)
  - Config file: `$XDG_CONFIG_HOME/orbitd.toml` (falls back to `$HOME/.config/orbitd.toml`)
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

## Advanced Architecture & Refactoring Learnings

### Trait Abstraction for Persistence
- **Persistence Trait**: Created `Persistence` trait in `orbit/src/persistence.rs` to abstract database operations
- **Trait Objects**: Use `Arc<dyn Persistence>` for shared ownership of trait objects
- **Error Standardization**: All persistence methods return `anyhow::Error` consistently
- **Module Separation**: Moved all persistence logic into dedicated module for better organization

### Business Logic Separation
- **Method Naming**: Business logic methods separated from FUSE trait implementation
- **Return Types**: Business logic methods return `Result<_, libc::c_int>` for proper FUSE error handling
- **FUSE Abstraction**: Filesystem trait implementations call business logic methods and handle protocol concerns
- **Public vs Private**: Business logic methods can be private since they're only used internally

### Type Safety & Data Structures
- **Error Types**: Use `libc::c_int` instead of `i32` for error codes to match FUSE expectations
- **Named Structs**: Created `DirectoryEntryInfo` struct to replace tuples in directory operations
- **Field Visibility**: Made `DirectoryEntry` fields public for testing and external access
- **Type Consistency**: Ensure all methods use consistent error and return types

## Critical Bug Fixes & Debugging Insights

### Network Synchronization Issues
- **Root Cause**: `update_directory_recursive` was loading old directory from persistence instead of runtime state
- **Persistence vs Runtime**: Persistence stores placeholder `InodeNumber(0)` values; runtime has real inode assignments
- **State Management**: Must use runtime state for old directories to preserve correct inode mappings
- **Inode Continuity**: Network sync must maintain inode number consistency to prevent "file not found" errors

### Parent Reference Management
- **Serialization Issue**: FsNodes loaded from persistence have `parent_inode_number: None` due to `#[serde(skip)]`
- **Manual Assignment**: Must explicitly set parent references when loading nodes from persistence
- **Directory Updates**: Both file nodes and directory nodes need parent references corrected during updates
- **Lookup Dependencies**: Proper parent references are critical for FUSE lookup and traversal operations

### Testing & Validation Strategies
- **Integration Tests**: Create tests that exercise persistence layer directly with `persist_fs_node`/`persist_directory`
- **Network Simulation**: Write tests that simulate network sync scenarios using `update_directory_recursive`
- **State Verification**: Test both `readdir`, `lookup`, and `getattr` operations after directory updates
- **Debug Tracing**: Add debug output to trace inode assignments and identify state inconsistencies

## FUSE Filesystem Implementation Details

### Operation Flow Understanding
- **ls Command Flow**: `ls` uses both `readdir` (list files) and `getattr`/`lookup` (get attributes)
- **Error Patterns**: Files visible in `readdir` but failing `lookup` indicates inode/state consistency issues
- **Error Codes**: Different FUSE error codes (`ENOENT`, `ENOTDIR`) have specific semantic meanings
- **Logging Strategy**: Remove unconditional debug logs, use warnings for actual errors

### Content-Addressable Storage Challenges
- **Dual Addressing**: Must maintain both content hashes (content addressing) and inode numbers (POSIX interface)
- **State Synchronization**: Persistence layer and runtime state serve different purposes and must stay coordinated
- **Network Coordination**: Distributed updates require careful sequencing of content persistence and inode management
- **Recovery Patterns**: Startup recovery (`restore_node_recursive`) works differently than live updates (`update_directory_recursive`)

## System Design Insights

### Distributed Filesystem Complexity
- **Multi-Phase Updates**: Network sync involves persist content → update directories → maintain inode consistency
- **State Invariants**: All state transitions must preserve filesystem invariants throughout the process
- **Dual Code Paths**: Both restart recovery and live update paths must correctly handle the same data
- **Consistency Models**: Runtime state represents current truth; persistence provides durability and sync capability
