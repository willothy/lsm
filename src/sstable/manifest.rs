use std::collections::BTreeMap;

use crate::{
    key::SeqNo,
    sstable::{manager::FileNo, Level},
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub next_file_number: FileNo,
    pub last_committed_sequence_number: SeqNo,

    levels: BTreeMap<Level, LevelMeta>,
}

impl Manifest {
    pub fn new() -> Self {
        // There is always at least level 0.
        //
        // L0 is special in that is the target for flushing memtables and also the
        // only level where files can overlap in key range.
        let levels = BTreeMap::from_iter(std::iter::once((
            Level(0),
            LevelMeta {
                level: Level(0),
                files: BTreeMap::new(),
            },
        )));

        Manifest {
            next_file_number: FileNo(0),
            last_committed_sequence_number: SeqNo::from(0u64),
            levels,
        }
    }

    #[must_use = "ManifestRecord must be logged to persist the allocation"]
    pub fn alloc_file_number(&mut self) -> (FileNo, ManifestRecord) {
        let id = self.next_file_number;
        self.next_file_number = id + 1;
        (id, ManifestRecord::AllocFileNumber(id))
    }

    pub fn load_from_file(file: &std::fs::File) -> Self {
        let reader = std::io::BufReader::new(file);

        let logs = crate::framed::read_all_framed::<_, ManifestRecord>(reader)
            .expect("Failed to read manifest records");

        // There is always at least level 0.
        //
        // L0 is special in that is the target for flushing memtables and also the
        // only level where files can overlap in key range.
        let levels = BTreeMap::from_iter(std::iter::once((
            Level(0),
            LevelMeta {
                level: Level(0),
                files: BTreeMap::new(),
            },
        )));

        let mut manifest = Manifest {
            next_file_number: FileNo(0),
            last_committed_sequence_number: SeqNo::from(0u64),
            levels,
        };

        for delta in logs {
            match delta {
                ManifestRecord::Snapshot(new_manifest) => {
                    manifest = new_manifest;
                }
                ManifestRecord::CreateFile { level, file_meta } => {
                    manifest
                        .levels
                        .entry(level)
                        .or_insert_with(|| LevelMeta {
                            level,
                            files: BTreeMap::new(),
                        })
                        .files
                        .insert(FileNo(file_meta.file_number), file_meta);
                }
                ManifestRecord::DeleteFile { level, file_number } => {
                    manifest
                        .levels
                        .entry(level)
                        .or_insert_with(|| LevelMeta {
                            level,
                            files: BTreeMap::new(),
                        })
                        .files
                        .remove(&FileNo(file_number));
                }
                ManifestRecord::SetLastSeqNo(seq_no) => {
                    manifest.last_committed_sequence_number = seq_no;
                }
                ManifestRecord::AllocFileNumber(file_no) => {
                    manifest.next_file_number = file_no.max(manifest.next_file_number);
                }
            }
        }

        manifest
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LevelMeta {
    pub level: Level,
    pub files: BTreeMap<FileNo, FileMeta>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileMeta {
    pub file_number: u64,
    pub file_size: u64,

    pub smallest_key: bytes::Bytes,
    pub largest_key: bytes::Bytes,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ManifestRecord {
    Snapshot(Manifest),
    /// Creates a new file in the manifest.
    CreateFile {
        level: Level,
        file_meta: FileMeta,
    },
    /// Deletes a file from the manifest.
    DeleteFile {
        level: Level,
        file_number: u64,
    },
    /// Sets the last committed sequence number
    SetLastSeqNo(SeqNo),
    /// Marks the allocation of a new file number.
    ///
    /// Set next_file_number to max(next_file_number, self.0).
    AllocFileNumber(FileNo),
}
