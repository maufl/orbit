# PFS UniFFI Bindings

This crate provides foreign language bindings for PFS using [UniFFI](https://mozilla.github.io/uniffi-rs/).

## Supported Languages

- Kotlin (JVM & Android)
- Swift (iOS & macOS)
- Python

## Building

Build the library:

```bash
cargo build --release -p pfs-uniffi
```

The compiled library will be at `target/release/libpfs_uniffi.so` (Linux), `libpfs_uniffi.dylib` (macOS), or `pfs_uniffi.dll` (Windows).

## Generating Kotlin Bindings

1. Install uniffi-bindgen-cli:
```bash
cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go
```

2. Generate Kotlin bindings:
```bash
cargo run --bin uniffi-bindgen generate --library target/release/libpfs_uniffi.so --language kotlin --out-dir bindings/kotlin
```

Or use the provided script:
```bash
./generate-bindings.sh kotlin
```

## Using in Kotlin

```kotlin
import uniffi.pfs_uniffi.*

// Create a PFS client with network communication
// This will generate a secret key if one doesn't exist
val client = PfsClient.new(
    dataDir = "/path/to/data",
    peerNodeIds = listOf(
        "abc123...",  // Hex-encoded peer node ID
        "def456..."
    )
)

// Or create without peer connections
val standaloneClient = PfsClient.new(
    dataDir = "/path/to/data",
    peerNodeIds = emptyList()
)

// The client automatically initializes the filesystem with networking enabled
// and connects to the specified peers
```

## Using in Swift

Generate Swift bindings:
```bash
cargo run --bin uniffi-bindgen generate --library target/release/libpfs_uniffi.dylib --language swift --out-dir bindings/swift
```

```swift
import pfs_uniffi

// Create a PFS client with network communication
let client = try PfsClient(
    dataDir: "/path/to/data",
    peerNodeIds: ["abc123...", "def456..."]
)
```

## Using in Python

Generate Python bindings:
```bash
cargo run --bin uniffi-bindgen generate --library target/release/libpfs_uniffi.so --language python --out-dir bindings/python
```

```python
from pfs_uniffi import PfsClient

# Create a PFS client with network communication
client = PfsClient.new(
    data_dir="/path/to/data",
    peer_node_ids=["abc123...", "def456..."]
)
```

## Current API

The API is intentionally minimal:

### Classes

- **`PfsClient`** - Main client for the distributed filesystem
  - `new(data_dir: String, peer_node_ids: Vec<String>)` - Initialize PFS with networking
    - Automatically creates data directory
    - Generates and persists a secret key if needed
    - Initializes network communication using iroh
    - Connects to specified peer nodes
    - Returns a fully initialized client ready for use via FUSE

### Errors

- **`PfsError`** - Error types for various operations
  - `IoError` - I/O operation failed
  - `InitializationError` - Failed to initialize PFS or networking

## Architecture

The `PfsClient` wraps a fully initialized `Pfs` instance with network communication enabled:

1. **Configuration**: Automatically manages config files and secret keys
2. **Networking**: Sets up iroh-based P2P networking with mDNS discovery
3. **Peer Connections**: Automatically connects to specified peers
4. **Tokio Runtime**: Manages async operations internally

The client is designed to be mounted as a FUSE filesystem after initialization.
For file operations, you would typically mount the filesystem and use standard file I/O.

## Extending the API

To add more functionality (e.g., file operations, directory listings):

1. Edit `src/lib.rs` and add new methods to `PfsClient` with `#[uniffi::export]` attribute
2. Rebuild the library
3. Regenerate the bindings for your target language

See the [UniFFI documentation](https://mozilla.github.io/uniffi-rs/latest/) for more details on supported types and patterns.
