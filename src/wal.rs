use std::{
    io::{Seek, Write},
    path::PathBuf,
};

use anyhow::Context;
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
        if let Err(e) = self.flush() {
            eprintln!("Failed to flush WAL on drop: {:?}", e);
        }

        if let Err(e) = self.file.unlock() {
            eprintln!("Failed to unlock WAL file on drop: {:?}", e);
        }
    }
}

impl Wal {
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .context("Failed to open WAL file")?;

        file.lock().context("Failed to lock WAL file")?;

        let (size, len) = Self::read_stats(&file)?;

        Ok(Wal { file, len, size })
    }

    pub fn should_compact(&self) -> bool {
        self.size > WAL_MAX_SIZE
    }

    fn read_stats(mut file: &std::fs::File) -> anyhow::Result<(u64, usize)> {
        let mut reader = std::io::BufReader::new(file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .context("seek to start")?;

        let mut len = 0;

        loop {
            match crate::framed::read_framed::<_, WalRecord>(&mut reader) {
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

        let offset = file.stream_position().context("Failed to get WAL size")?;

        Ok((offset, len))
    }

    pub fn append(&mut self, record: WalRecord) -> anyhow::Result<()> {
        let written = crate::framed::write_framed(&mut self.file, &record)
            .context("Failed to serialize WAL record")?;

        self.size += written as u64;
        self.len += 1;

        self.flush()?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn replay(&mut self) -> anyhow::Result<Vec<WalRecord>> {
        let mut reader = std::io::BufReader::new(&self.file);

        reader
            .seek(std::io::SeekFrom::Start(0))
            .context("seek to start")?;

        Ok(crate::framed::read_all_framed::<_, WalRecord>(&mut reader)
            .context("Failed to read WAL records")?)
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.file.flush().context("Failed to flush WAL")?;
        self.file.sync_all().context("Failed to sync WAL")?;

        Ok(())
    }

    pub fn clear(&mut self) -> anyhow::Result<()> {
        self.file
            .set_len(0)
            .context("Failed to truncate WAL for clear")?;

        self.len = 0;
        self.size = 0;

        Ok(())
    }
}
