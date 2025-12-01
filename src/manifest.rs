use crate::key::SeqNo;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Level {
    L1,
    L2,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    next_file_number: u64,
    pub last_committed_sequence_number: SeqNo,

    l1_meta: LevelMeta,
    l2_meta: LevelMeta,
}

impl Manifest {
    pub fn load_from_path(path: &std::path::PathBuf) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("Failed to open manifest file");

        let reader = std::io::BufReader::new(file);

        let logs = crate::log::read_all_framed::<_, ManifestRecord>(reader)
            .expect("Failed to read manifest records");

        let mut manifest = Manifest {
            next_file_number: 0,
            last_committed_sequence_number: SeqNo::from(0u64),
            l1_meta: LevelMeta {
                level: Level::L1,
                files: Vec::new(),
            },
            l2_meta: LevelMeta {
                level: Level::L2,
                files: Vec::new(),
            },
        };

        for delta in logs {
            match delta {
                ManifestRecord::Snapshot(new_manifest) => {
                    manifest = new_manifest;
                }
                ManifestRecord::CreateFile { level, file_meta } => match level {
                    Level::L1 => manifest.l1_meta.files.push(file_meta),
                    Level::L2 => manifest.l2_meta.files.push(file_meta),
                },
                ManifestRecord::DeleteFile { level, file_number } => match level {
                    Level::L1 => manifest
                        .l1_meta
                        .files
                        .retain(|f| f.file_number != file_number),
                    Level::L2 => manifest
                        .l2_meta
                        .files
                        .retain(|f| f.file_number != file_number),
                },
                ManifestRecord::SetLastSeqNo(seq_no) => {
                    manifest.last_committed_sequence_number = seq_no;
                }
            }
        }

        manifest
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LevelMeta {
    pub level: Level,
    pub files: Vec<FileMeta>,
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
    CreateFile { level: Level, file_meta: FileMeta },
    DeleteFile { level: Level, file_number: u64 },
    SetLastSeqNo(SeqNo),
}
