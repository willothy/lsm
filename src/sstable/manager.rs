use std::io::{Read, Write};

use anyhow::Context;

use crate::{
    key::SeqNo,
    sstable::{
        manifest::{Manifest, ManifestRecord},
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
    pub fn open(manifests_dir: &std::path::Path) -> anyhow::Result<Self> {
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

            let mut current_manfiest_name = String::new();
            current_file
                .read_to_string(&mut current_manfiest_name)
                .context("Failed to read current manifest name from CURRENT file")?;

            let current_manifest_file = std::fs::OpenOptions::new()
                .create(false)
                .read(true)
                .append(true)
                .open(manifests_dir.join(&current_manfiest_name))
                .context("Failed to open current manifest file")?;

            current_manifest_file
                .lock()
                .context("Failed to lock current manifest file")?;

            let manifest = Manifest::load_from_file(&current_manifest_file)?;

            (current_file, current_manifest_file, manifest)
        };

        Ok(SSTableManager {
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
}
