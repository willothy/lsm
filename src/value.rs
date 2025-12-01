#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Data(bytes::Bytes),
    Tombstone,
}
