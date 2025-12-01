use std::{
    io::{BufWriter, Seek, Write},
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

impl WalRecord {
    pub fn key(&self) -> &Key {
        match self {
            WalRecord::Put { key, .. } => key,
            WalRecord::Delete { key } => key,
        }
    }
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

    fn read_stats(mut file: &std::fs::File) -> (u64, usize) {
        let mut reader = std::io::BufReader::new(file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek to start");

        let mut len = 0;

        loop {
            match crate::log::read_framed::<_, WalRecord>(&mut reader) {
                Ok(_) => {
                    len += 1;
                }
                Err(e) => match e {
                    postcard::Error::DeserializeUnexpectedEnd => {
                        break;
                    }
                    e => panic!("{e}"),
                },
            };
        }

        let offset = file.stream_position().expect("Failed to get WAL size");

        (offset, len)
    }

    pub fn append(&mut self, record: WalRecord) {
        let written = crate::log::write_framed(BufWriter::new(&mut self.file), &record)
            .expect("Failed to serialize WAL record");

        self.size += written as u64;
        self.len += 1;

        self.flush();
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn replay(&mut self) -> Vec<WalRecord> {
        let mut reader = std::io::BufReader::new(&self.file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .expect("seek to start");

        crate::log::read_all_framed::<_, WalRecord>(&mut reader)
            .expect("Failed to read WAL records")
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
