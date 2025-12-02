use std::{
    io::{Read, Seek, Write},
    rc::Rc,
};

use anyhow::Context;
use bytes::BufMut;

use crate::{
    config::Config,
    key::{Key, SeqNo},
    memtable::state::Frozen,
    sstable::{
        manifest::{FileMeta, Manifest, ManifestRecord},
        sstable::{index_block_size, BlockMeta, SSTableFooter, BLOCK_SIZE},
        Level,
    },
};

#[derive(
    Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct FileNo(pub u64);

impl std::ops::Add<u64> for FileNo {
    type Output = FileNo;

    fn add(self, rhs: u64) -> Self::Output {
        FileNo(self.0 + rhs)
    }
}

impl std::fmt::Display for FileNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// L0 (base) SSTable file size (64MB).
pub const BASE_LEVEL_SIZE: usize = 1024 * 1024 * 64;
/// SSTable size ratio. Each level's file size is determined by [`BASE_LEVEL_SIZE`] * [`SIZE_RATIO`]^2.
pub const SIZE_RATIO: usize = 10;

pub fn calculate_sstable_size(level: &Level) -> usize {
    BASE_LEVEL_SIZE * SIZE_RATIO.pow(level.0)
}

pub const CURRENT_FILE_NAME: &str = "CURRENT";
pub const MANIFEST_FILE_EXT: &str = "manifest";
pub const SSTABLE_FILE_EXT: &str = "sstable";

pub fn format_file_name(id: FileNo, ext: &str) -> String {
    format!("{id:06}.{ext}")
}

#[derive(Debug)]
pub struct SSTableManager {
    config: Rc<crate::config::Config>,

    /// Locked [`CURRENT_FILE_NAME`] file acts as a pointer to the active [`Manifest`].
    current: std::fs::File,

    active_file: std::fs::File,
    active_manifest: Manifest,
}

impl Drop for SSTableManager {
    fn drop(&mut self) {
        self.current.unlock().ok();
        self.active_file.unlock().ok();
    }
}

impl SSTableManager {
    pub fn open(config: Rc<Config>) -> anyhow::Result<Self> {
        let manifests_dir = config.data_dir.join("manifests");
        let current_file_path = manifests_dir.join(CURRENT_FILE_NAME);

        let (current_file, active_file, active_manifest) = if !current_file_path
            .try_exists()
            .is_ok_and(|readable| readable)
        {
            let (current_file, active_file, active_manifest) = if manifests_dir
                .read_dir()
                .context("Failed to read manifest dir")?
                .next()
                .is_none()
            {
                // This is probably a new DB, create a new manifest and CURRENT file
                let mut current_file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(&current_file_path)
                    .context("Failed to create CURRENT file")?;

                current_file.lock().context("Failed to lock CURRENT file")?;

                let mut manifest = Manifest::new();

                // We don't need the alloc record since we're writing a snapshot immediately
                let (initial_manifest_id, _) = manifest.alloc_file_number();
                let initial_manifest_name =
                    format_file_name(initial_manifest_id, MANIFEST_FILE_EXT);

                current_file
                    .set_len(0)
                    .context("Failed to truncate CURRENT file")?;
                current_file
                    .write_all(initial_manifest_name.as_bytes())
                    .context("Failed to write initial manifest id to CURRENT file")?;

                current_file
                    .flush()
                    .context("Failed to flush CURRENT file")?;
                current_file
                    .sync_all()
                    .context("Failed to sync CURRENT file")?;

                let mut active_file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(manifests_dir.join(&initial_manifest_name))
                    .context("Failed to create first manifest")?;

                active_file
                    .lock()
                    .context("Failed to lock active manifest file")?;

                crate::framed::write_framed(
                    &mut active_file,
                    &ManifestRecord::Snapshot(manifest.clone()),
                )
                .context("Failed to write initial manifest snapshot")?;

                active_file
                    .flush()
                    .context("Failed to flush active manifest file")?;
                active_file
                    .sync_all()
                    .context("Failed to sync active manifest file")?;

                (current_file, active_file, manifest)
            } else {
                panic!("CURRENT file not detected but manifests were found");
            };

            (current_file, active_file, active_manifest)
        } else {
            let mut current_file = std::fs::OpenOptions::new()
                .create(false)
                .read(true)
                .write(true)
                .open(&current_file_path)
                .context("Failed to open CURRENT file")?;

            current_file.lock().context("Failed to lock CURRENT file")?;

            let mut current_manifest = String::new();
            current_file
                .read_to_string(&mut current_manifest)
                .context("Failed to read current manifest name from CURRENT file")?;

            let current_manifest_file = std::fs::OpenOptions::new()
                .create(false)
                .read(true)
                .append(true)
                .open(manifests_dir.join(&current_manifest))
                .context("Failed to open current manifest file")?;

            current_manifest_file
                .lock()
                .context("Failed to lock current manifest file")?;

            let manifest = Manifest::load_from_file(&current_manifest_file)?;

            (current_file, current_manifest_file, manifest)
        };

        Ok(SSTableManager {
            config,

            current: current_file,

            active_file,
            active_manifest,
        })
    }

    fn append_record(&mut self, record: ManifestRecord) -> anyhow::Result<()> {
        crate::framed::write_framed(&mut self.active_file, &record)
            .context("Failed to append record")?;

        Ok(())
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.active_file
            .flush()
            .context("Failed to flush active manifest file")?;

        self.active_file
            .sync_all()
            .context("Failed to fsync active manifest file")?;

        Ok(())
    }

    pub fn alloc_file_number(&mut self) -> anyhow::Result<FileNo> {
        let (fileno, record) = self.active_manifest.alloc_file_number();

        self.append_record(record)?;

        self.sync()?;

        Ok(fileno)
    }

    pub fn last_committed_sequence_number(&self) -> SeqNo {
        self.active_manifest.last_committed_sequence_number
    }

    fn finalize_sstable(
        &mut self,
        file: &mut std::fs::File,
        file_no: FileNo,
        first_key: &Key,
        last_key: &Key,
        block_meta: &[BlockMeta],
    ) -> anyhow::Result<()> {
        let mut index_buf = bytes::BytesMut::with_capacity(index_block_size(&block_meta));
        let index_start = file.stream_position()?;

        index_buf.put_u32_le(block_meta.len() as u32);

        for meta in block_meta {
            meta.last_key.encode_into(&mut index_buf);
            index_buf.put_u64_le(meta.offset);
            index_buf.put_u32_le(meta.size);
        }

        file.write_all(&index_buf)?;

        file.seek(std::io::SeekFrom::Start(
            (BASE_LEVEL_SIZE - std::mem::size_of::<SSTableFooter>()) as u64,
        ))?;

        let footer = SSTableFooter {
            index_offset: index_start,
            index_size: index_buf.len() as u64,
            _reserved1: 0,
            _reserved2: 0,
            magic: 0xDEAD_BEEF,
        };

        index_buf.clear();

        footer.encode_into(&mut index_buf);

        file.write_all(&index_buf)?;

        file.flush()?;
        file.sync_all()?;

        self.append_record(ManifestRecord::CreateFile {
            level: Level(0),
            file_meta: FileMeta {
                file_number: file_no.0,
                file_size: BASE_LEVEL_SIZE as u64,
                smallest_key: first_key.encode_to_bytes(),
                largest_key: last_key.encode_to_bytes(),
            },
        })?;

        Ok(())
    }

    pub fn flush_memtable(
        &mut self,
        memtable: &crate::memtable::MemTable<Frozen>,
    ) -> anyhow::Result<()> {
        let mut file_no = self.alloc_file_number()?;
        let mut file = {
            let file_name = format_file_name(file_no, SSTABLE_FILE_EXT);

            std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&self.config.data_dir.join("sstables").join(file_name))
                .context("Failed to create SSTable file")?
        };

        let mut block_meta = Vec::new();
        let mut current_block = bytes::BytesMut::with_capacity(BLOCK_SIZE);
        let mut sstable_size = 0u64;

        file.seek(std::io::SeekFrom::Start(0))?;

        let mut entry_buf = bytes::BytesMut::with_capacity(128);

        let mut first_key = None;
        let mut last_key = None;

        for (key, val) in memtable.data() {
            first_key = Some(first_key.unwrap_or_else(|| key.clone()));

            entry_buf.clear();

            key.encode_into(&mut entry_buf);
            val.encode_into(&mut entry_buf);

            if current_block.len() + entry_buf.len() >= BLOCK_SIZE {
                file.write_all(&current_block)?;

                current_block.clear();

                block_meta.push(BlockMeta {
                    last_key: last_key.clone().expect(
                        "There should be at least one key in the block if we're writing it",
                    ),
                    offset: file.stream_position()?,
                    size: current_block.len() as u32,
                });

                sstable_size += BLOCK_SIZE as u64;
            }

            if sstable_size
                + BLOCK_SIZE as u64
                + index_block_size(&block_meta) as u64
                + std::mem::size_of::<SSTableFooter>() as u64
                > (BASE_LEVEL_SIZE as u64)
            {
                self.finalize_sstable(
                    &mut file,
                    file_no,
                    first_key.as_ref().expect("smallest key"),
                    last_key.as_ref().expect("largest key"),
                    &block_meta,
                )?;

                block_meta.clear();
                current_block.clear();
                sstable_size = 0;
                first_key = None;

                file_no = self.alloc_file_number()?;
                let new_file_name = format_file_name(file_no, SSTABLE_FILE_EXT);
                file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(&self.config.data_dir.join("sstables").join(new_file_name))
                    .context("Failed to create new SSTable file")?;
            }

            current_block.put_slice(&entry_buf);

            last_key = Some(key.clone());
        }

        self.finalize_sstable(
            &mut file,
            file_no,
            first_key.as_ref().expect("smallest key"),
            last_key.as_ref().expect("largest key"),
            &block_meta,
        )?;

        Ok(())
    }
}
