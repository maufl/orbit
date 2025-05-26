# Peer file system

Needs a root node, which is always a directory.

Supported file types are directory, regular file and symlink.
Things to ignore for now: permissions.

A file system node consists of

```rust
enum FileType {
	Directory,
	RegularFile,
	Symlink
}
 
struct FileNode {
	/// (recursive?) size in bytes
	size: u64,
	/// Time of last change to the content
	modification_time: DateTime,
	/// The hash of the content, SHA256 probably
	content_hash: Hash,
	/// Kind of file
	kind: FileType,
}
```

Each file system node has a hash value too.
A directory entry is just a name and a hash of a file system node.
```rust
struct DirectoryEntry {
	name: String,
	file_node_hash: Hash,
}
 
struct Directory {
	entries: Vec<DirectoryEntry>,
};
```

To store this information during run time we have the following structs
```rust
let mut root_node: FileNode = _;
let mut file_nodes: BTreeMap<Hash, FileNode> = _;
let mut directories: BTreeMap<Hash, Directory> = _;
```

The changes to the file system as a whole are tracked in a block chain. It is implemented as a DAG like this
```rust
struct FileSystem {
	current_root_node: Hash,
	parent_root_nodes: Vec<Hash>,
}
```
We implement opening, reading and writing by actually opening a file in the underlying file system. If we open it writable, we make a copy first. We store opened files in a vector and use the index as a file handle.

```rust
use std::fs::File;

struct OpenFile {
	path: Path,
	underlying_file: File,
	writable: boolean,
}

let mut open_files: Vec<Option<OpenFile>>;
```