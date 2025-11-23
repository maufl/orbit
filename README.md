# Orbit

A distributed content-addressed filesystem with peer-to-peer synchronization. The system implements a FUSE-based filesystem on Linux and a Storage Access Framework provider on Android, both backed by the same Rust core library.

## Architecture

### Storage Model

Files are stored by SHA-256 content hash, enabling automatic deduplication. Metadata is persisted in an embedded fjall key-value store using CBOR serialization. The system maintains a blockchain-style history where each block references a root directory hash and its predecessor.

### Components

**orbit** - Core Rust library (~3,115 lines)
- `OrbitFs` - Main filesystem implementation with thread-safe runtime state using `Arc<parking_lot::RwLock<OrbitFsRuntimeData>>`
- Persistence layer using fjall embedded database with separate partitions for nodes, directories, and blocks
- Network communication via iroh with mDNS-based local discovery
- FUSE interface implementing standard POSIX operations

**orbitd** - Linux daemon binary
- Mounts filesystem at configured location (default: `$HOME/Orbit`)
- Manages peer connections and synchronization
- Handles network events and content transfer

**orbit-android** - UniFFI bindings for Android/Kotlin integration
- Exposes path-based API without FUSE dependency
- Methods: `get_node_by_path()`, `list_directory()`, `read_file()`, `write_file()`

**android** - Android application
- `OrbitService` - Foreground service maintaining persistent operation
- `OrbitDocumentProvider` - Storage Access Framework integration
- Jetpack Compose UI for file browsing and peer discovery
- Background workers for service persistence across device restarts

### Synchronization Protocol

Peers discover each other via mDNS on local networks. Synchronization operates through:

1. Block hash exchange to determine current state
2. Common ancestor identification via block chain traversal
3. Diff calculation for missing filesystem changes
4. Transfer of blocks, directory structures, and file nodes
5. Content request/response for actual file data
6. Recursive directory updates preserving inode consistency

Conflict detection identifies divergent histories. Three-way merge is not yet implemented.

## Dependencies

**Storage:** fjall (LSM-tree database), ciborium (CBOR), serde
**Networking:** iroh v0.95.1 (P2P), tokio (async runtime)
**Filesystem:** fuser (FUSE bindings), nix (Unix syscalls)
**Concurrency:** parking_lot (RwLock implementation)
**Cross-platform:** uniffi v0.29 (FFI generation)

## Build

### Linux

```bash
# Quick compilation check
cargo check

# Development build
cargo build

# Release build
cargo build --release

# Run daemon
cargo run --bin orbitd

# With configuration file
cargo run --bin orbitd -- --config /path/to/config.toml

# Connect to peer
cargo run --bin orbitd -- <remote_node_id>
```

### Android

```bash
cd android
./gradlew assembleDebug      # Build APK
./gradlew installDebug       # Install on connected device
```

Android build requires:
- Android NDK
- Python 3.x for UniFFI binding generation
- `rust-android-gradle` plugin handles cross-compilation

## Configuration

Configuration file location (TOML format):
- `$XDG_CONFIG_HOME/orbitd.toml`
- Fallback: `$HOME/.config/orbitd.toml`

Default paths:
- Mount point: `$HOME/Orbit`
- Data directory: `$XDG_DATA_HOME/orbit_data` (fallback: `$HOME/.local/share/orbit_data`)
- Metadata: `{data_dir}/metadata/`

Private keys are generated automatically on first run and persisted in the configuration.

## Testing

```bash
# Integration tests (must run single-threaded due to FUSE)
cargo test --test basics -- --test-threads=1

# Network synchronization tests
cargo test --test network -- --test-threads=1

# Format code after changes
rustfmt orbit/src/**/*.rs
```

Tests create temporary filesystems with automatic cleanup. Each test uses UUID-based naming to prevent conflicts.

## Network Discovery

On local networks, nodes announce themselves via mDNS with service type `_orbit._tcp`. Device name is used as the advertised node name. Discovery is automatic when nodes are on the same network segment.

Manual connection requires the remote node's public key identifier.

## Data Structures

```rust
struct FsNode {
    size: u64,
    modification_time: chrono::DateTime<Utc>,
    content_hash: ContentHash,         // SHA-256
    kind: FsNodeKind,                  // Directory, RegularFile, Symlink
    inode_number: InodeNumber,         // POSIX inode for FUSE
    parent_inode_number: Option<InodeNumber>,
}

struct Directory {
    entries: BTreeMap<String, DirectoryEntry>,
}

struct DirectoryEntry {
    name: String,
    fs_node_hash: FsNodeHash,
    inode_number: InodeNumber,
}

struct Block {
    root_fs_node_hash: FsNodeHash,
    previous_block_hash: Option<BlockHash>,
    timestamp: chrono::DateTime<Utc>,
}
```

Runtime state maintains mappings between:
- Inode numbers → FsNodes (for POSIX compatibility)
- Content hashes → File data (for deduplication)
- FsNode hashes → FsNodes (for persistence)
- Directory hashes → Directories (for synchronization)

## Limitations

- Network synchronization requires manual conflict resolution for divergent histories
- Android implementation does not expose FUSE mount (platform limitation)
- No garbage collection for unreferenced content
- No storage quotas or selective sync
- Synchronization is all-or-nothing for entire filesystem tree

## AI disclaimer
I was/am and skeptic, but I started using Claude Code for this and it's really helpful.
Some or most of the code in this repository might be generated using AI.
If that's what it takes for me to actually implement this dream of mine, so be it.
