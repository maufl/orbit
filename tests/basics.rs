use chrono::Utc;
use fuser::BackgroundSession;
use pfs::{ContentHash, Directory, DirectoryEntry, FileType, FsNode, FsNodeHash, InodeNumber, Pfs};
use std::fs;
use std::io::Write;
use std::thread;
use std::time::Duration;

fn setup_pfs() -> Pfs {
    let uuid = uuid::Uuid::new_v4();
    let data_dir = format!("/tmp/{}/pfs_data", uuid);
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    Pfs::initialize(data_dir, None).expect("Failed to initialize filesystem")
}

fn setup() -> (BackgroundSession, String, Pfs) {
    let uuid = uuid::Uuid::new_v4();
    let mount_point = format!("/tmp/{}/pfs", uuid);
    std::fs::create_dir_all(&mount_point).expect("To create the mount point");
    let fs = setup_pfs();
    let guard = fuser::spawn_mount2(fs.clone(), &mount_point, &vec![]).unwrap();
    (guard, mount_point, fs)
}

fn create_file_with_content(file_path: &str, content: &[u8]) {
    let mut file = fs::File::create_new(file_path).expect("To create file");
    file.write_all(content).expect("To write to file");
    file.flush().expect("To flush file");
}

fn create_test_file_node(size: u64) -> FsNode {
    FsNode {
        size,
        modification_time: Utc::now(),
        content_hash: ContentHash::default(),
        kind: FileType::RegularFile,
    }
}

fn create_test_dir_node(content_hash: ContentHash) -> FsNode {
    FsNode {
        size: 0,
        modification_time: Utc::now(),
        content_hash,
        kind: FileType::Directory,
    }
}

fn create_test_directory(entries: Vec<(&str, FsNodeHash)>) -> Directory {
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

#[test]
fn create_hello_world_file() {
    let (guard, mount_point, _fs) = setup();

    // Wait for filesystem to be ready
    thread::sleep(Duration::from_millis(100));

    let uuid = uuid::Uuid::new_v4();
    let file_path = format!("{}/{}", mount_point, uuid);

    // Create and write to file
    {
        let mut file = fs::File::create_new(&file_path).expect("To create file");
        file.write_all(b"Hello World!").expect("To write to file");
        file.flush().expect("To flush file");
    } // File handle is dropped here

    // Wait for file operations to complete
    thread::sleep(Duration::from_millis(100));

    // Read file content
    let content = fs::read_to_string(&file_path).expect("To read file");
    assert_eq!(content, "Hello World!");

    drop(guard);
}

#[test]
fn create_and_read_multiple_files() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let files = vec![
        ("file1.txt", "Content of file 1"),
        ("file2.txt", "Different content for file 2"),
        ("file3.txt", "Yet another file with different content"),
    ];

    // Create files
    for (filename, content) in &files {
        let file_path = format!("{}/{}", mount_point, filename);
        {
            let mut file = fs::File::create_new(&file_path).expect("To create file");
            file.write_all(content.as_bytes())
                .expect("To write to file");
            file.flush().expect("To flush file");
        }
        thread::sleep(Duration::from_millis(50));
    }

    // Read and verify files
    for (filename, expected_content) in &files {
        let file_path = format!("{}/{}", mount_point, filename);
        let actual_content = fs::read_to_string(&file_path).expect("To read file");
        assert_eq!(actual_content, *expected_content);
    }

    drop(guard);
}

#[test]
fn create_directory() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let dir_path = format!("{}/test_dir", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");

    thread::sleep(Duration::from_millis(100));

    // Verify directory exists
    let metadata = fs::metadata(&dir_path).expect("To get directory metadata");
    assert!(metadata.is_dir());

    drop(guard);
}

#[test]
fn create_nested_directories() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create first level directory
    let dir1_path = format!("{}/dir1", mount_point);
    fs::create_dir(&dir1_path).expect("To create first directory");
    thread::sleep(Duration::from_millis(50));

    // Create second level directory
    let dir2_path = format!("{}/dir1/dir2", mount_point);
    fs::create_dir(&dir2_path).expect("To create second directory");
    thread::sleep(Duration::from_millis(50));

    // Verify both directories exist
    assert!(fs::metadata(&dir1_path).unwrap().is_dir());
    assert!(fs::metadata(&dir2_path).unwrap().is_dir());

    drop(guard);
}

#[test]
fn create_file_in_directory() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create directory
    let dir_path = format!("{}/test_dir", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");
    thread::sleep(Duration::from_millis(50));

    // Create file in directory
    let file_path = format!("{}/test_file.txt", dir_path);
    {
        let mut file = fs::File::create_new(&file_path).expect("To create file in directory");
        file.write_all(b"File in directory")
            .expect("To write to file");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(50));

    // Verify file content
    let content = fs::read_to_string(&file_path).expect("To read file");
    assert_eq!(content, "File in directory");

    drop(guard);
}

#[test]
fn list_directory_contents() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create some files and directories
    let items = vec![("file1.txt", false), ("file2.txt", false), ("subdir", true)];

    for (name, is_dir) in &items {
        let path = format!("{}/{}", mount_point, name);
        if *is_dir {
            fs::create_dir(&path).expect("To create directory");
        } else {
            let mut file = fs::File::create_new(&path).expect("To create file");
            file.write_all(b"test content").expect("To write to file");
            file.flush().expect("To flush file");
        }
        thread::sleep(Duration::from_millis(50));
    }

    // List directory contents
    let entries: Vec<_> = fs::read_dir(&mount_point)
        .expect("To read directory")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();

    // Verify all items are present
    for (name, _) in &items {
        assert!(entries.contains(&name.to_string()), "Missing: {}", name);
    }

    drop(guard);
}

#[test]
fn write_and_read_empty_file() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let file_path = format!("{}/empty_file.txt", mount_point);

    // Create empty file
    {
        let file = fs::File::create_new(&file_path).expect("To create empty file");
        drop(file);
    }
    thread::sleep(Duration::from_millis(50));

    // Read empty file
    let content = fs::read_to_string(&file_path).expect("To read empty file");
    assert_eq!(content, "");

    drop(guard);
}

#[test]
fn write_large_file() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let file_path = format!("{}/large_file.txt", mount_point);
    let large_content = "x".repeat(200_000); // 200KB file

    // Create large file
    {
        let mut file = fs::File::create_new(&file_path).expect("To create large file");
        file.write_all(large_content.as_bytes())
            .expect("To write large content");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(200));

    // Read and verify large file
    let content = fs::read_to_string(&file_path).expect("To read large file");
    assert_eq!(content, large_content);
    assert_eq!(content.len(), 200_000);

    drop(guard);
}

#[test]
fn delete_file() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let file_path = format!("{}/delete_me.txt", mount_point);
    let test_content = "This file will be deleted";

    // Create and write to file
    {
        let mut file = fs::File::create_new(&file_path).expect("To create file");
        file.write_all(test_content.as_bytes())
            .expect("To write to file");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(50));

    // Verify file exists and has correct content
    assert!(fs::metadata(&file_path).unwrap().is_file());
    let content = fs::read_to_string(&file_path).expect("To read file");
    assert_eq!(content, test_content);

    // Delete the file
    fs::remove_file(&file_path).expect("To delete file");
    thread::sleep(Duration::from_millis(50));

    // Verify file no longer exists
    assert!(fs::metadata(&file_path).is_err());

    drop(guard);
}

#[test]
fn rename_file() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    let old_file_path = format!("{}/old_name.txt", mount_point);
    let new_file_path = format!("{}/new_name.txt", mount_point);
    let test_content = "This file will be renamed";

    // Create and write to file
    {
        let mut file = fs::File::create_new(&old_file_path).expect("To create file");
        file.write_all(test_content.as_bytes())
            .expect("To write to file");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(50));

    // Verify file exists with old name
    assert!(fs::metadata(&old_file_path).unwrap().is_file());
    let content = fs::read_to_string(&old_file_path).expect("To read file");
    assert_eq!(content, test_content);

    // Rename the file
    fs::rename(&old_file_path, &new_file_path).expect("To rename file");
    thread::sleep(Duration::from_millis(50));

    // Verify old file no longer exists
    assert!(fs::metadata(&old_file_path).is_err());

    // Verify new file exists and has correct content
    assert!(fs::metadata(&new_file_path).unwrap().is_file());
    let content = fs::read_to_string(&new_file_path).expect("To read renamed file");
    assert_eq!(content, test_content);

    drop(guard);
}

#[test]
fn rename_file_to_different_directory() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create a subdirectory
    let subdir_path = format!("{}/subdir", mount_point);
    fs::create_dir(&subdir_path).expect("To create subdirectory");
    thread::sleep(Duration::from_millis(50));

    let old_file_path = format!("{}/file.txt", mount_point);
    let new_file_path = format!("{}/subdir/moved_file.txt", mount_point);
    let test_content = "This file will be moved to subdirectory";

    // Create and write to file in root
    {
        let mut file = fs::File::create_new(&old_file_path).expect("To create file");
        file.write_all(test_content.as_bytes())
            .expect("To write to file");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(50));

    // Verify file exists in root
    assert!(fs::metadata(&old_file_path).unwrap().is_file());

    // Move file to subdirectory with new name
    fs::rename(&old_file_path, &new_file_path).expect("To move file to subdirectory");
    thread::sleep(Duration::from_millis(50));

    // Verify old file no longer exists in root
    assert!(fs::metadata(&old_file_path).is_err());

    // Verify new file exists in subdirectory and has correct content
    assert!(fs::metadata(&new_file_path).unwrap().is_file());
    let content = fs::read_to_string(&new_file_path).expect("To read moved file");
    assert_eq!(content, test_content);

    drop(guard);
}

#[test]
fn rename_file_to_existing_file() {
    let (guard, mount_point, _fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create first file
    let file1_path = format!("{}/file1.txt", mount_point);
    let file1_content = "This is file 1";
    {
        let mut file = fs::File::create_new(&file1_path).expect("To create file1");
        file.write_all(file1_content.as_bytes())
            .expect("To write to file1");
        file.flush().expect("To flush file1");
    }
    thread::sleep(Duration::from_millis(50));

    // Create second file
    let file2_path = format!("{}/file2.txt", mount_point);
    let file2_content = "This is file 2";
    {
        let mut file = fs::File::create_new(&file2_path).expect("To create file2");
        file.write_all(file2_content.as_bytes())
            .expect("To write to file2");
        file.flush().expect("To flush file2");
    }
    thread::sleep(Duration::from_millis(50));

    // Verify both files exist with correct content
    assert!(fs::metadata(&file1_path).unwrap().is_file());
    assert!(fs::metadata(&file2_path).unwrap().is_file());
    let content1 = fs::read_to_string(&file1_path).expect("To read file1");
    let content2 = fs::read_to_string(&file2_path).expect("To read file2");
    assert_eq!(content1, file1_content);
    assert_eq!(content2, file2_content);

    // Try to rename file1 to file2 (this should succeed and overwrite file2)
    let rename_result = fs::rename(&file1_path, &file2_path);
    
    // This should succeed - the filesystem now allows overwriting
    assert!(rename_result.is_ok(), "Rename should succeed and overwrite target file");
    
    // Verify file1 no longer exists
    assert!(fs::metadata(&file1_path).is_err(), "Original file should no longer exist after rename");
    
    // Verify file2 now exists and has file1's content
    assert!(fs::metadata(&file2_path).unwrap().is_file(), "Target file should still exist");
    let content2_after = fs::read_to_string(&file2_path).expect("To read file2 after rename");
    assert_eq!(content2_after, file1_content, "File2 should now have file1's content");

    drop(guard);
}

#[test]
fn test_persistence_infrastructure() {
    let uuid = uuid::Uuid::new_v4();
    let mount_point = format!("/tmp/{}/pfs", uuid);
    let data_dir = format!("/tmp/{}/pfs_data", uuid);

    // Create directories
    std::fs::create_dir_all(&mount_point).expect("To create the mount point");
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");

    let test_files = vec![
        ("persistent_file1.txt", "This content should be stored"),
        ("persistent_file2.txt", "Another file with content"),
    ];
    let test_dir = "persistent_dir";

    // First filesystem session - create files and directories
    {
        let fs = Pfs::initialize(data_dir.clone(), None).expect("Failed to initialize filesystem");
        let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
        thread::sleep(Duration::from_millis(100));

        // Create test files
        for (filename, content) in &test_files {
            let file_path = format!("{}/{}", mount_point, filename);
            {
                let mut file = fs::File::create_new(&file_path).expect("To create file");
                file.write_all(content.as_bytes())
                    .expect("To write to file");
                file.flush().expect("To flush file");
            }
            thread::sleep(Duration::from_millis(50));
        }

        // Create test directory
        let dir_path = format!("{}/{}", mount_point, test_dir);
        fs::create_dir(&dir_path).expect("To create directory");
        thread::sleep(Duration::from_millis(50));

        // Verify files exist in first session
        for (filename, expected_content) in &test_files {
            let file_path = format!("{}/{}", mount_point, filename);
            let content = fs::read_to_string(&file_path).expect("To read file");
            assert_eq!(content, *expected_content);
        }

        // Verify directory exists
        let dir_path = format!("{}/{}", mount_point, test_dir);
        assert!(fs::metadata(&dir_path).unwrap().is_dir());

        drop(guard);
    }

    // Small delay to ensure clean unmount
    thread::sleep(Duration::from_millis(200));

    // Verify that persistence files were created
    let metadata_dir = format!("{}/metadata", data_dir);
    assert!(
        std::path::Path::new(&metadata_dir).exists(),
        "Metadata directory should exist"
    );

    // Check that fjall database files exist (fjall creates several files)
    let metadata_contents = fs::read_dir(&metadata_dir)
        .expect("To read metadata directory")
        .count();
    assert!(
        metadata_contents > 0,
        "Metadata directory should contain fjall database files"
    );

    // Second filesystem session - verify the persistence infrastructure works
    {
        let fs = Pfs::initialize(data_dir.clone(), None).expect("Failed to initialize filesystem");
        let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
        thread::sleep(Duration::from_millis(100));

        // Verify all original files are present after restart
        for (filename, expected_content) in &test_files {
            let file_path = format!("{}/{}", mount_point, filename);
            let content = fs::read_to_string(&file_path)
                .expect(&format!("To read restored file: {}", filename));
            assert_eq!(
                content, *expected_content,
                "File {} content should be restored",
                filename
            );
        }

        // Verify original directory exists after restart
        let dir_path = format!("{}/{}", mount_point, test_dir);
        assert!(
            fs::metadata(&dir_path).unwrap().is_dir(),
            "Directory should be restored"
        );

        // Create a new file to verify the filesystem is functional
        let new_file_path = format!("{}/new_after_restart.txt", mount_point);
        {
            let mut file =
                fs::File::create_new(&new_file_path).expect("To create file after restart");
            file.write_all(b"Created after restart")
                .expect("To write to new file");
            file.flush().expect("To flush new file");
        }
        thread::sleep(Duration::from_millis(50));

        let new_content = fs::read_to_string(&new_file_path).expect("To read new file");
        assert_eq!(new_content, "Created after restart");

        drop(guard);
    }

    // Verify persistence data still exists after second session
    assert!(
        std::path::Path::new(&metadata_dir).exists(),
        "Metadata should persist across sessions"
    );

    // Cleanup
    std::fs::remove_dir_all(&format!("/tmp/{}", uuid)).ok();
}

#[test]
fn test_diff_added_files() {
    let (guard, mount_point, fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Capture initial root state
    let initial_root = fs.get_root_node();

    // Add a new file
    let file_path = format!("{}/new_file.txt", mount_point);
    create_file_with_content(&file_path, b"content");
    thread::sleep(Duration::from_millis(50));

    // Capture new root state
    let new_root = fs.get_root_node();

    // Test diff
    let (fs_nodes, directories) = fs.diff(&initial_root.into(), &new_root.into());

    // Should detect the new file and updated root directory
    assert!(fs_nodes.len() == 2, "Should detect at least new file");
    assert!(
        directories.len() == 1,
        "Should detect at least updated root directory"
    );

    // Check that one of the fs_nodes is a regular file
    let file_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::RegularFile)
        .collect();
    assert_eq!(file_nodes.len(), 1, "Should have exactly one new file");

    drop(guard);
}

#[test]
fn test_diff_changed_files() {
    let (guard, mount_point, fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create initial file
    let file_path = format!("{}/test_file.txt", mount_point);
    create_file_with_content(&file_path, b"initial content");
    thread::sleep(Duration::from_millis(50));

    // Capture root state after initial file creation
    let initial_root = fs.get_root_node();

    // Create a different file with different content (simulates file change)
    fs::remove_file(&file_path).expect("To remove original file");
    create_file_with_content(&file_path, b"modified content");
    thread::sleep(Duration::from_millis(50));

    // Capture new root state
    let new_root = fs.get_root_node();

    // Test diff
    let (fs_nodes, directories) = fs.diff(&initial_root.into(), &new_root.into());

    println!("FsNodes {:?}", fs_nodes);
    println!("Directories {:?}", directories);
    // Should detect the changed file and updated root directory
    assert!(
        fs_nodes.len() == 2,
        "Should detect changed file and updated root"
    );
    assert!(
        directories.len() == 1,
        "Should detect at least one directory"
    );

    // Check that at least one of the fs_nodes is a regular file
    let file_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::RegularFile)
        .collect();
    assert!(file_nodes.len() >= 1, "Should have at least one file");

    drop(guard);
}

#[test]
fn test_diff_added_directories() {
    let (guard, mount_point, fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Capture initial root state
    let initial_root = fs.get_root_node();

    // Add a new directory
    let dir_path = format!("{}/new_directory", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");
    thread::sleep(Duration::from_millis(50));

    // Capture new root state
    let new_root = fs.get_root_node();

    // Test diff
    let (fs_nodes, directories) = fs.diff(&initial_root.into(), &new_root.into());

    // Should detect the new directory and updated root directory
    assert!(fs_nodes.len() == 2, "Should detect at least new directory");
    assert!(
        directories.len() == 2,
        "Should detect at least one directory"
    );

    // Check that one of the fs_nodes is a directory
    let dir_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::Directory)
        .collect();
    assert!(
        dir_nodes.len() >= 1,
        "Should have at least one directory node"
    );

    drop(guard);
}

#[test]
fn test_diff_changed_directories() {
    let (guard, mount_point, fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create initial directory structure
    let dir_path = format!("{}/test_directory", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");
    thread::sleep(Duration::from_millis(50));

    // Capture root state after directory creation
    let initial_root = fs.get_root_node();

    // Add a file to the directory (this changes the directory)
    let file_path = format!("{}/file_in_dir.txt", dir_path);
    create_file_with_content(&file_path, b"content in directory");
    thread::sleep(Duration::from_millis(50));

    // Capture new root state
    let new_root = fs.get_root_node();

    // Test diff
    let (fs_nodes, directories) = fs.diff(&initial_root.into(), &new_root.into());

    // Should detect the new file, changed directory, and updated root directory
    assert_eq!(
        fs_nodes.len(),
        3,
        "Should detect at least the new file and updated structures"
    );
    assert_eq!(
        directories.len(),
        2,
        "Should detect at least the changed directory and updated root"
    );

    // Check that we have both file and directory nodes
    let file_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::RegularFile)
        .collect();
    let dir_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::Directory)
        .collect();

    assert!(file_nodes.len() >= 1, "Should have at least one file node");
    assert!(
        dir_nodes.len() >= 1,
        "Should have at least one directory node"
    );

    drop(guard);
}

#[test]
fn test_diff_comprehensive_scenario() {
    let (guard, mount_point, fs) = setup();
    thread::sleep(Duration::from_millis(100));

    // Create initial state
    let dir1_path = format!("{}/dir1", mount_point);
    fs::create_dir(&dir1_path).expect("To create dir1");

    let file1_path = format!("{}/file1.txt", mount_point);
    create_file_with_content(&file1_path, b"file1 content");
    thread::sleep(Duration::from_millis(100));

    // Capture initial root state
    let initial_root = fs.get_root_node();

    // Make multiple changes:
    // 1. Add a new file
    let file2_path = format!("{}/file2.txt", mount_point);
    create_file_with_content(&file2_path, b"file2 content");

    // 2. Add a new directory
    let dir2_path = format!("{}/dir2", mount_point);
    fs::create_dir(&dir2_path).expect("To create dir2");

    // 3. Add a file to existing directory (changes the directory)
    let file_in_dir1_path = format!("{}/file_in_dir1.txt", dir1_path);
    create_file_with_content(&file_in_dir1_path, b"content in dir1");

    // 4. Simulate file modification by replacing it
    fs::remove_file(&file1_path).expect("To remove original file1");
    create_file_with_content(&file1_path, b"modified file1 content");

    thread::sleep(Duration::from_millis(100));

    // Capture new root state
    let new_root = fs.get_root_node();

    // Test diff
    let (fs_nodes, directories) = fs.diff(&initial_root.into(), &new_root.into());

    // Verify we detected the changes
    assert!(
        fs_nodes.len() >= 5,
        "Should detect multiple new/changed nodes"
    );
    assert!(
        directories.len() >= 3,
        "Should detect multiple directory changes"
    );

    // Check that we have both file and directory nodes
    let file_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::RegularFile)
        .collect();
    let dir_nodes: Vec<_> = fs_nodes
        .iter()
        .filter(|n| n.kind == FileType::Directory)
        .collect();

    assert!(file_nodes.len() >= 2, "Should have multiple file nodes");
    assert!(dir_nodes.len() >= 2, "Should have multiple directory nodes");

    drop(guard);
}

#[test]
fn test_update_directory_recursive_with_persistence() {
    let mut fs = setup_pfs();

    // Get the initial empty root directory
    let initial_root_node = fs.get_root_node();
    let initial_root_hash = initial_root_node.calculate_hash();

    // Create a new file node with default content hash
    let file_node = create_test_file_node(100);

    // Persist the file node
    fs.persistence
        .persist_fs_node(&file_node)
        .expect("To persist file node");

    // Create a directory that contains the file
    let new_directory = create_test_directory(vec![("test_file.txt", file_node.calculate_hash())]);

    // Persist the directory
    fs.persistence
        .persist_directory(&new_directory)
        .expect("To persist directory");

    // Create a new directory node that references our directory
    let new_root_node = create_test_dir_node(new_directory.calculate_hash());

    // Persist the new root node
    fs.persistence
        .persist_fs_node(&new_root_node)
        .expect("To persist new root node");

    let new_root_hash = new_root_node.calculate_hash();

    // Now use update_directory_recursive to update the root folder
    fs.update_directory_recursive(&initial_root_hash, &new_root_hash, InodeNumber(1))
        .expect("To update directory recursive");

    // Verify the update worked by checking the root node
    let updated_root = fs.get_root_node();
    assert_eq!(updated_root.content_hash, new_directory.calculate_hash());

    // Since we can't access private methods, let's verify by using the public diff method
    // to see that our changes are properly detected
    let initial_root_fs_node: FsNode = initial_root_node.into();
    let updated_root_fs_node: FsNode = updated_root.into();
    let (_fs_nodes, directories) = fs.diff(&initial_root_fs_node, &updated_root_fs_node);

    // We should have at least one directory change (the root directory)
    assert!(!directories.is_empty(), "Should have directory changes");

    // The last directory in the list should be our new root directory
    let new_root_directory = &directories[directories.len() - 1];
    assert_eq!(
        new_root_directory.entries.len(),
        1,
        "Should have exactly one entry in the new directory"
    );

    // Verify the directory calculates to the same hash as our new directory
    assert_eq!(
        new_root_directory.calculate_hash(),
        new_directory.calculate_hash(),
        "Directory hashes should match"
    );

    // Most importantly, verify that the root content hash changed from empty to containing our file
    assert_ne!(
        initial_root_node.content_hash, updated_root.content_hash,
        "Root content hash should have changed"
    );
    assert_eq!(
        updated_root.content_hash,
        new_directory.calculate_hash(),
        "Root should now contain our new directory"
    );

    // Test filesystem operations on the updated root directory

    // 1. Use pfs_readdir to list contents of root folder (inode 1)
    let dir_entries = fs.pfs_readdir(1, 0).expect("To read root directory");

    // Should have entries: ".", "..", and "test_file.txt"
    assert_eq!(
        dir_entries.len(),
        3,
        "Should have 3 entries: ., .., and test_file.txt"
    );

    // Find our file entry
    let file_entry = dir_entries
        .iter()
        .find(|entry| entry.name == "test_file.txt")
        .expect("Should find test_file.txt in directory listing");

    assert_eq!(
        file_entry.file_type,
        fuser::FileType::RegularFile,
        "Should be a regular file"
    );
    let file_inode = file_entry.ino;

    // 2. Use pfs_lookup to find the file by name
    let lookup_attrs = fs
        .pfs_lookup(1, "test_file.txt")
        .expect("To lookup test_file.txt");
    assert_eq!(
        lookup_attrs.ino, file_inode,
        "Lookup should return same inode as readdir"
    );
    assert_eq!(
        lookup_attrs.kind,
        fuser::FileType::RegularFile,
        "Lookup should show regular file"
    );
    assert_eq!(
        lookup_attrs.size, 100,
        "File size should be 100 as we set it"
    );

    // 3. Use pfs_getattr to get file attributes by inode
    let file_attrs = fs.pfs_getattr(file_inode).expect("To get file attributes");
    assert_eq!(
        file_attrs.ino, file_inode,
        "Getattr should return correct inode"
    );
    assert_eq!(
        file_attrs.kind,
        fuser::FileType::RegularFile,
        "Getattr should show regular file"
    );
    assert_eq!(
        file_attrs.size, 100,
        "Getattr should show correct file size"
    );

    // Verify that lookup and getattr return consistent information
    assert_eq!(
        lookup_attrs.ino, file_attrs.ino,
        "Lookup and getattr should agree on inode"
    );
    assert_eq!(
        lookup_attrs.size, file_attrs.size,
        "Lookup and getattr should agree on size"
    );
    assert_eq!(
        lookup_attrs.kind, file_attrs.kind,
        "Lookup and getattr should agree on file type"
    );
}

#[test]
fn test_update_directory_recursive_preserves_parent_references() {
    let mut fs = setup_pfs();

    // Create initial directory with a file
    let file1_node = create_test_file_node(50);
    fs.persistence
        .persist_fs_node(&file1_node)
        .expect("To persist file1 node");

    let initial_directory = create_test_directory(vec![("file1.txt", file1_node.calculate_hash())]);
    fs.persistence
        .persist_directory(&initial_directory)
        .expect("To persist initial directory");

    let initial_root_node = create_test_dir_node(initial_directory.calculate_hash());
    fs.persistence
        .persist_fs_node(&initial_root_node)
        .expect("To persist initial root node");

    // Set up the initial state
    let initial_root_hash = initial_root_node.calculate_hash();
    fs.update_directory_recursive(
        &fs.get_root_node().calculate_hash(),
        &initial_root_hash,
        InodeNumber(1),
    )
    .expect("To set up initial state");

    // Now simulate updating the file (like network synchronization would do)
    let updated_file1_node = create_test_file_node(75); // Changed size
    fs.persistence
        .persist_fs_node(&updated_file1_node)
        .expect("To persist updated file1 node");

    let updated_directory =
        create_test_directory(vec![("file1.txt", updated_file1_node.calculate_hash())]);
    fs.persistence
        .persist_directory(&updated_directory)
        .expect("To persist updated directory");

    let updated_root_node = create_test_dir_node(updated_directory.calculate_hash());
    fs.persistence
        .persist_fs_node(&updated_root_node)
        .expect("To persist updated root node");

    // Update directory recursively (like network sync would do)
    let updated_root_hash = updated_root_node.calculate_hash();
    fs.update_directory_recursive(&initial_root_hash, &updated_root_hash, InodeNumber(1))
        .expect("To update directory recursive");

    // Now verify that the file can be properly accessed
    let dir_entries = fs.pfs_readdir(1, 0).expect("To read root directory");
    let file_entry = dir_entries
        .iter()
        .find(|entry| entry.name == "file1.txt")
        .expect("Should find file1.txt in directory listing");

    // The key test: lookup should work (this would fail if parent_inode_number was None)
    let lookup_attrs = fs
        .pfs_lookup(1, "file1.txt")
        .expect("To lookup file1.txt - this tests parent reference");
    assert_eq!(lookup_attrs.size, 75, "Should show updated file size");
    assert_eq!(
        lookup_attrs.ino, file_entry.ino,
        "Lookup and readdir should agree on inode"
    );

    // Verify getattr also works
    let file_attrs = fs
        .pfs_getattr(file_entry.ino)
        .expect("To get file attributes");
    assert_eq!(file_attrs.size, 75, "Getattr should show updated file size");
}

#[test]
fn test_network_sync_scenario() {
    let mut fs = setup_pfs();

    // Simulate the initial state - empty filesystem
    let initial_root = fs.get_root_node();

    // Now simulate receiving a file from network sync
    let file_node = create_test_file_node(1024);

    // Network would persist this file
    fs.persistence
        .persist_fs_node(&file_node)
        .expect("To persist file node");

    // Network would persist the directory containing this file
    let new_directory =
        create_test_directory(vec![("synced_file.txt", file_node.calculate_hash())]);
    fs.persistence
        .persist_directory(&new_directory)
        .expect("To persist directory");

    // Network would persist the new root node
    let new_root_node = create_test_dir_node(new_directory.calculate_hash());
    fs.persistence
        .persist_fs_node(&new_root_node)
        .expect("To persist new root node");

    // Now simulate the network calling update_directory_recursive
    let old_root_hash = initial_root.calculate_hash();
    let new_root_hash = new_root_node.calculate_hash();

    fs.update_directory_recursive(&old_root_hash, &new_root_hash, InodeNumber(1))
        .expect("To update directory recursive");

    // Now test that ls-like operations work:

    // 1. readdir should show the file
    let entries = fs.pfs_readdir(1, 0).expect("To read directory");
    let file_entry = entries
        .iter()
        .find(|e| e.name == "synced_file.txt")
        .expect("Should find synced_file.txt");

    println!("File entry inode: {}", file_entry.ino);

    // 2. lookup should work (this is what ls does for each file)
    let lookup_result = fs.pfs_lookup(1, "synced_file.txt");
    match lookup_result {
        Ok(attrs) => {
            println!("Lookup succeeded: inode={}, size={}", attrs.ino, attrs.size);
            assert_eq!(attrs.size, 1024, "Should show correct size");
        }
        Err(e) => {
            panic!(
                "Lookup failed with error code: {} (this is the ls problem!)",
                e
            );
        }
    }

    // 3. getattr should work on the inode
    let getattr_result = fs.pfs_getattr(file_entry.ino);
    match getattr_result {
        Ok(attrs) => {
            println!(
                "Getattr succeeded: inode={}, size={}",
                attrs.ino, attrs.size
            );
            assert_eq!(attrs.size, 1024, "Should show correct size");
        }
        Err(e) => {
            panic!(
                "Getattr failed with error code: {} (this would cause ls issues!)",
                e
            );
        }
    }
}

#[test]
fn test_network_sync_existing_file_update() {
    let mut fs = setup_pfs();

    // Start by creating a file in the normal way (like local filesystem operations)
    let initial_root = fs.get_root_node();

    // Create the first version of the file locally
    let file_v1_node = create_test_file_node(500);
    fs.persistence
        .persist_fs_node(&file_v1_node)
        .expect("To persist file v1");

    let directory_v1 =
        create_test_directory(vec![("shared_file.txt", file_v1_node.calculate_hash())]);
    fs.persistence
        .persist_directory(&directory_v1)
        .expect("To persist directory v1");

    let root_v1_node = create_test_dir_node(directory_v1.calculate_hash());
    fs.persistence
        .persist_fs_node(&root_v1_node)
        .expect("To persist root v1");

    // Set up the initial state with the file
    let root_v1_hash = root_v1_node.calculate_hash();
    fs.update_directory_recursive(
        &initial_root.calculate_hash(),
        &root_v1_hash,
        InodeNumber(1),
    )
    .expect("To set up initial state with file");

    // Verify the file works initially
    let entries = fs.pfs_readdir(1, 0).expect("To read directory initially");
    let file_entry_v1 = entries
        .iter()
        .find(|e| e.name == "shared_file.txt")
        .expect("Should find shared_file.txt initially");

    println!("Initial file inode: {}", file_entry_v1.ino);

    fs.pfs_lookup(1, "shared_file.txt")
        .expect("Initial lookup should work");
    fs.pfs_getattr(file_entry_v1.ino)
        .expect("Initial getattr should work");

    // Now simulate network sync updating this same file (same name, different content)
    let file_v2_node = create_test_file_node(750); // Different size
    fs.persistence
        .persist_fs_node(&file_v2_node)
        .expect("To persist file v2");

    let directory_v2 =
        create_test_directory(vec![("shared_file.txt", file_v2_node.calculate_hash())]); // Same name, different hash!
    fs.persistence
        .persist_directory(&directory_v2)
        .expect("To persist directory v2");

    let root_v2_node = create_test_dir_node(directory_v2.calculate_hash());
    fs.persistence
        .persist_fs_node(&root_v2_node)
        .expect("To persist root v2");

    // This is the critical test: update the directory with the new version
    let root_v2_hash = root_v2_node.calculate_hash();
    fs.update_directory_recursive(&root_v1_hash, &root_v2_hash, InodeNumber(1))
        .expect("To update to v2 via network sync");

    // Now test if the updated file is accessible (this is where the bug manifests)
    let entries_v2 = fs
        .pfs_readdir(1, 0)
        .expect("To read directory after update");
    let file_entry_v2 = entries_v2
        .iter()
        .find(|e| e.name == "shared_file.txt")
        .expect("Should find shared_file.txt after update");

    println!("Updated file inode: {}", file_entry_v2.ino);

    // Debug: Let's check if the issue is with inode 0
    if file_entry_v2.ino == 0 {
        println!("WARNING: File has inode 0, which might be invalid!");
    }

    // The critical test: can we look up the updated file?
    let lookup_result = fs.pfs_lookup(1, "shared_file.txt");
    match lookup_result {
        Ok(attrs) => {
            println!(
                "Lookup after update succeeded: inode={}, size={}",
                attrs.ino, attrs.size
            );
            assert_eq!(attrs.size, 750, "Should show updated size");

            // Also test getattr
            let getattr_result = fs.pfs_getattr(attrs.ino);
            match getattr_result {
                Ok(getattr_attrs) => {
                    println!(
                        "Getattr after update succeeded: size={}",
                        getattr_attrs.size
                    );
                    assert_eq!(
                        getattr_attrs.size, 750,
                        "Getattr should also show updated size"
                    );
                }
                Err(e) => {
                    panic!("Getattr failed after update with error: {}", e);
                }
            }
        }
        Err(e) => {
            panic!(
                "Lookup failed after update with error code: {} - THIS IS THE BUG!",
                e
            );
        }
    }
}
