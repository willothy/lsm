use std::path::PathBuf;

pub const BLOCK_SIZE: usize = 1024 * 16; // 16 KB

pub struct BlockMeta {
    pub(crate) last_key: crate::key::Key,
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

#[repr(C)]
pub struct SSTableFooter {
    pub(crate) index_offset: u64,
    pub(crate) index_size: u64,
    pub(crate) _reserved1: u64,
    pub(crate) _reserved2: u32,
    pub(crate) magic: u32,
}

impl SSTableFooter {
    pub fn encode_into(&self, mut buf: impl bytes::BufMut) {
        buf.put_u64_le(self.index_offset);
        buf.put_u64_le(self.index_size);
        buf.put_u64_le(self._reserved1);
        buf.put_u32_le(self._reserved2);
        buf.put_u32_le(self.magic);
    }

    pub fn decode_from(mut buf: impl bytes::Buf) -> Self {
        let index_offset = buf.get_u64_le();
        let index_size = buf.get_u64_le();
        let _reserved1 = buf.get_u64_le();
        let _reserved2 = buf.get_u32_le();
        let magic = buf.get_u32_le();

        SSTableFooter {
            index_offset,
            index_size,
            _reserved1,
            _reserved2,
            magic,
        }
    }
}

pub fn index_block_size(entries: &[BlockMeta]) -> usize {
    // Each entry consists of:
    // - last_key (variable size)
    // - offset (8 bytes)
    // - size (4 bytes)
    let entries: usize = entries
        .iter()
        .map(|e| e.last_key.encoded_len() + 8 + 4)
        .sum();

    entries + 4 /* length (u32) */
}

pub struct SSTable {
    path: PathBuf,
    mem: memmap2::Mmap,
}
