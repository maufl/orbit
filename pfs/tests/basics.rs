use pfs::test_utils::*;
use pfs::Pfs;
use std::fs;
use std::io::Write;
use std::thread;
use std::time::Duration;

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
    assert!(
        rename_result.is_ok(),
        "Rename should succeed and overwrite target file"
    );

    // Verify file1 no longer exists
    assert!(
        fs::metadata(&file1_path).is_err(),
        "Original file should no longer exist after rename"
    );

    // Verify file2 now exists and has file1's content
    assert!(
        fs::metadata(&file2_path).unwrap().is_file(),
        "Target file should still exist"
    );
    let content2_after = fs::read_to_string(&file2_path).expect("To read file2 after rename");
    assert_eq!(
        content2_after, file1_content,
        "File2 should now have file1's content"
    );

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
