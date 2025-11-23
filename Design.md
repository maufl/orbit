# System Design

## Overview

Orbit implements a distributed content-addressed filesystem using a layered architecture separating content storage, metadata persistence, and interface layers. The system provides POSIX filesystem semantics on Linux via FUSE and path-based access on Android via Storage Access Framework.

## Core Data Structures

### FsNode

Represents a filesystem object (file, directory, or symlink). Contains:
- Size and modification timestamp
- SHA-256 content hash (file data or directory structure)
- Node type (directory, regular file, symlink)
- Runtime-assigned POSIX inode number
- Parent inode reference (not persisted, reconstructed on load)
- Extended attributes for custom metadata

### Directory

Maps filenames to filesystem node hashes. Entries include:
- Filename
- FsNode hash reference
- Inode number for POSIX compatibility

### Block

Represents a filesystem state snapshot. Forms a linked list (blockchain) where each block contains:
- Root directory hash
- Previous block hash (null for genesis)
- Timestamp

This creates an append-only history of filesystem states enabling synchronization and version tracking.

### Hash Types

Four distinct hash types prevent type confusion:
- ContentHash: SHA-256 of file content
- FsNodeHash: SHA-256 of serialized FsNode
- DirectoryHash: SHA-256 of serialized Directory
- BlockHash: SHA-256 of serialized Block

All hashes are 32-byte SHA-256 digests.

## Architecture Layers

### Layer 1: Content Storage

Files stored on disk with base64-encoded content hash as filename. Properties:
- Automatic deduplication (identical content stored once)
- Immutable (content never modified after creation)
- Content-addressed (filename derived from SHA-256)

### Layer 2: Persistence

Embedded LSM-tree database (fjall) stores metadata using CBOR serialization. Separate partitions for:
- Filesystem nodes (by FsNode hash)
- Directories (by Directory hash)
- Blocks (by Block hash)
- Special key for current root hash

Provides history diff operation: given a block hash, return all subsequent blocks.

### Layer 3: Runtime State

In-memory data structures maintain active filesystem state:
- Inode-to-FsNode mapping (POSIX compatibility)
- Inode-to-Directory mapping (traversal)
- Open file handles

Read-write locks enable concurrent read operations. Initialization loads latest block from persistence and recursively restores entire directory tree, assigning inodes and reconstructing parent references.

### Layer 4: Interface

**FUSE (Linux):** Standard filesystem operations (lookup, getattr, readdir, mkdir, create, write, unlink, etc.). Each operation acquires appropriate locks, performs business logic, persists changes, creates new block if needed, and triggers network sync.

**Path-Based API (Android):** Path-oriented operations (get_node_by_path, list_directory, read_file, write_file, create_directory, remove). Exposed to Kotlin via UniFFI foreign function interface. Used by Storage Access Framework DocumentProvider.

## Network Synchronization

### Discovery

Peers discover each other via mDNS on local networks, announcing as `{device_name}._orbit._tcp.local`. Manual connection via public key also supported.

### Protocol

CBOR-serialized messages exchanged over QUIC connections:
- Block hash notifications
- History requests/responses
- FsNode and Directory requests/responses
- File content requests/responses

### Synchronization Flow

1. **Connection Establishment:** Peer discovered, QUIC connection established, bi-directional stream opened.

2. **State Exchange:** Peers exchange current block hashes. If peer's block missing locally, request history.

3. **Ancestry Check:** Walk backward through block chains to find common ancestor. If none found, histories diverged (conflict detected).

4. **Content Transfer:** For each missing block, recursively request all referenced nodes, directories, and file content. Receiver persists all data.

5. **Directory Update:** Merge remote directory structure into runtime state, preserving existing inode numbers for unchanged files, assigning new inodes for additions.

6. **Block Commit:** Create new block referencing updated root, persist, notify peers.

### Conflict Detection

Common ancestor search determines if histories diverged. Three-way merge not yet implemented; system logs warning on divergence.

## File Operations

**Read:** Lookup inode, extract content hash, open content file by hash.

**Write:** On first write, create temporary copy. Accumulate writes. On close, calculate new content hash, persist content file, update FsNode, update parent directory, create block, trigger sync.

**Create:** Assign inode, create FsNode, add to parent directory, persist, create block, trigger sync.

**Delete:** Remove from parent directory, persist directory, create block, trigger sync. Content file not deleted (no garbage collection).

## State Management

### Dual State Model

**Persistence Layer:** Stores all nodes and directories by hash for durability and synchronization. Parent references not serialized.

**Runtime State:** Maps inodes to nodes for FUSE operations. Contains current filesystem truth. Parent references reconstructed.

### Startup Recovery

Latest block loaded from persistence. Directory tree recursively restored by loading root node, then recursively loading all children. Inode numbers assigned during traversal. Parent references set as tree is traversed.

### Live Updates

Remote directory structures merged into runtime state by comparing old and new entries. Unchanged files preserve existing inodes. Modified or new files get new inodes. Removed files deleted from runtime state. Recursive for nested directories.

**Critical Invariants:**
- Runtime state uses existing inodes for unchanged files (POSIX stability)
- Parent references set on all loaded nodes (traversal correctness)
- Directory updates use runtime state for old directories, not persistence (inode preservation)
- Persistence stores serialized inodes; runtime assigns actual values

## Android Integration

### UniFFI Bindings

Foreign function interface generates Kotlin code from Rust definitions. Exposes path-based API to Android without FUSE dependency.

### Storage Access Framework

DocumentProvider translates Android storage operations to Orbit path-based API. Document IDs are filesystem paths. Provides integration with Android file pickers and document-based apps.

### Background Service

Foreground service maintains persistent operation. Displays notification to prevent Android process termination. WorkManager schedules periodic checks to restart service if killed.

## RPC Interface

Unix socket-based RPC server using tarpc library. Socket path configurable, defaults to XDG runtime directory. Currently provides single method: get_node_id returns node's public key as hex string. Runs concurrently with filesystem operations.
