use pfs::test_utils::*;
use pfs::{FileType, FsNode, InodeNumber};
use std::fs;
use std::thread;
use std::time::Duration;

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
    let (fs_nodes, directories) = fs.persistence.diff(&initial_root.into(), &new_root.into());

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
    let (fs_nodes, directories) = fs.persistence.diff(&initial_root.into(), &new_root.into());

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
    let (fs_nodes, directories) = fs.persistence.diff(&initial_root.into(), &new_root.into());

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
    let (fs_nodes, directories) = fs.persistence.diff(&initial_root.into(), &new_root.into());

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
    let (fs_nodes, directories) = fs.persistence.diff(&initial_root.into(), &new_root.into());

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
    let (_fs_nodes, directories) = fs
        .persistence
        .diff(&initial_root_fs_node, &updated_root_fs_node);

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
