pub mod manager;
pub mod manifest;
pub mod sstable;

#[derive(
    Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Level(pub u32);
