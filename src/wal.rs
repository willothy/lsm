use std::{
    io::{Seek, Write},
    path::PathBuf,
};

use bytes::Bytes;

use crate::key::Key;

const WAL_MAX_SIZE: u64 = 1024 * 64 /* 64KB */;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WalRecord {
    Put { key: Key, val: Bytes },
    Delete { key: Key },
}

pub struct Wal {
    file: std::fs::File,
    /// The size of the WAL file *NOT* including trailing zeros from pre-allocation.
    size: u64,
    /// The number of records in the WAL.
    len: usize,
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.flush();

        self.file.unlock().expect("Failed to unlock WAL file");
    }
}

impl Wal {
    const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard()
        .with_little_endian()
        .with_no_limit()
        .with_variable_int_encoding();

    pub fn new(path: PathBuf) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .expect("Failed to open WAL file");

        file.lock().expect("Failed to lock WAL file");

        let (size, len) = Self::read_stats(&file);

        Wal { file, len, size }
    }

    pub fn should_compact(&self) -> bool {
        self.size > WAL_MAX_SIZE
    }

    fn read_stats(file: &std::fs::File) -> (u64, usize) {
        let mut reader = std::io::BufReader::new(file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek to start");

        let mut len = 0;

        loop {
            match bincode::serde::decode_from_reader::<WalRecord, _, _>(
                &mut reader,
                Self::BINCODE_CONFIG,
            ) {
                Ok(_) => {
                    len += 1;
                }
                Err(e) => match e {
                    bincode::error::DecodeError::UnexpectedEnd { .. } => break,
                    bincode::error::DecodeError::Io { inner, .. }
                        if matches!(inner.kind(), std::io::ErrorKind::UnexpectedEof) =>
                    {
                        break
                    }
                    _ => panic!("Invalid WAL record"),
                },
            }
        }

        let offset = reader
            .stream_position()
            .expect("failed to read stream position");

        (offset, len)
    }

    pub fn append(&mut self, record: WalRecord) {
        let written =
            bincode::serde::encode_into_std_write(&record, &mut self.file, Self::BINCODE_CONFIG)
                .expect("Failed to append to WAL");

        self.size += written as u64;
        self.len += 1;

        self.flush();
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn replay(&self) -> Vec<WalRecord> {
        let mut res = Vec::new();
        let mut reader = std::io::BufReader::new(&self.file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek to start");

        loop {
            let record = match bincode::serde::decode_from_reader(&mut reader, Self::BINCODE_CONFIG)
            {
                Ok(r) => r,
                Err(e) => match e {
                    bincode::error::DecodeError::UnexpectedEnd { .. } => break,
                    bincode::error::DecodeError::Io { inner, .. }
                        if matches!(inner.kind(), std::io::ErrorKind::UnexpectedEof) =>
                    {
                        break
                    }
                    _ => panic!("Invalid WAL record"),
                },
            };

            res.push(record);
        }

        res
    }

    pub fn flush(&mut self) {
        self.file.flush().expect("Failed to flush WAL");
        self.file.sync_all().expect("Failed to sync WAL");
    }

    pub fn clear(&mut self) {
        self.file
            .set_len(0)
            .expect("Failed to truncate WAL for clear");

        self.len = 0;
        self.size = 0;
    }
}
