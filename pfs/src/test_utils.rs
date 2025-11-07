use crate::{ContentHash, Directory, DirectoryEntry, FileType, FsNode, FsNodeHash, Pfs};
use chrono::Utc;
use fuser::BackgroundSession;
use std::fs;
use std::io::Write;

pub fn setup_pfs() -> Pfs {
    let uuid = uuid::Uuid::new_v4();
    let data_dir = format!("/tmp/{}/pfs_data", uuid);
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    Pfs::initialize(data_dir, None).expect("Failed to initialize filesystem")
}

pub fn setup() -> (BackgroundSession, String, Pfs) {
    let uuid = uuid::Uuid::new_v4();
    let mount_point = format!("/tmp/{}/pfs", uuid);
    std::fs::create_dir_all(&mount_point).expect("To create the mount point");
    let fs = setup_pfs();
    let guard = fuser::spawn_mount2(fs.clone(), &mount_point, &vec![]).unwrap();
    (guard, mount_point, fs)
}

pub fn create_file_with_content(file_path: &str, content: &[u8]) {
    let mut file = fs::File::create_new(file_path).expect("To create file");
    file.write_all(content).expect("To write to file");
    file.flush().expect("To flush file");
}

pub fn create_test_file_node(size: u64) -> FsNode {
    FsNode {
        size,
        modification_time: Utc::now(),
        content_hash: ContentHash::default(),
        kind: FileType::RegularFile,
    }
}

pub fn create_test_dir_node(content_hash: ContentHash) -> FsNode {
    FsNode {
        size: 0,
        modification_time: Utc::now(),
        content_hash,
        kind: FileType::Directory,
    }
}

pub fn create_test_directory(entries: Vec<(&str, FsNodeHash)>) -> Directory {
    Directory {
        entries: entries
            .into_iter()
            .map(|(name, hash)| DirectoryEntry {
                name: name.to_string(),
                fs_node_hash: hash,
            })
            .collect(),
    }
}
