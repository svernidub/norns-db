use crate::{Journal, JournalRecordData};

#[tokio::test]
async fn test_commit_creates_commit_file_with_committed_offset() {
    let dir = std::env::temp_dir()
        .join("norns_db_journal_tests")
        .join("commit_creates_commit_file");

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let wal_path = dir.join("wal.log");
    let journal = Journal::<String, String>::new(&wal_path).unwrap();

    let record = JournalRecordData::Upsert {
        key: "key".to_string(),
        value: "value".to_string(),
    };

    let handle = journal.append(record).await.unwrap();
    let expected_offset = handle.offset;

    let commit_path = wal_path.with_extension("wal.commit");
    assert!(
        !commit_path.exists(),
        "commit file should not exist before commit"
    );

    journal.commit(handle).await.unwrap();

    assert!(
        commit_path.exists(),
        "commit file should exist after commit"
    );

    let bytes = std::fs::read(&commit_path).unwrap();
    let committed_offset = u64::from_le_bytes(bytes.try_into().unwrap());
    assert_eq!(committed_offset, expected_offset);
}
