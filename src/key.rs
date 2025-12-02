use bytes::{Buf, BufMut};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SeqNo(pub u64);

impl std::ops::Add<u64> for SeqNo {
    type Output = SeqNo;

    fn add(mut self, rhs: u64) -> Self::Output {
        self.0 += rhs;
        self
    }
}

impl From<u64> for SeqNo {
    fn from(value: u64) -> Self {
        SeqNo(value)
    }
}

impl Into<u64> for SeqNo {
    fn into(self) -> u64 {
        self.0
    }
}

impl SeqNo {
    pub fn next(&mut self) -> SeqNo {
        let cur = SeqNo(self.0);
        self.0 += 1;
        cur
    }

    pub fn skip(&mut self, n: u64) {
        self.0 += n;
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Key(bytes::Bytes, SeqNo);

impl Key {
    pub fn new(user_key: bytes::Bytes, seqno: SeqNo) -> Self {
        Key(user_key, seqno)
    }

    pub fn min_seqno(user_key: bytes::Bytes) -> Self {
        Key(user_key, SeqNo(u64::MAX))
    }

    pub fn max_seqno(user_key: bytes::Bytes) -> Self {
        Key(user_key, SeqNo(0))
    }

    pub fn range_by_user_key(user_key: bytes::Bytes) -> std::ops::RangeInclusive<Self> {
        Key::min_seqno(user_key.clone())..=Key::max_seqno(user_key)
    }

    pub fn with_seqno(&self, seqno: SeqNo) -> Self {
        Key(self.0.clone(), seqno)
    }

    pub fn user_key(&self) -> &bytes::Bytes {
        &self.0
    }

    pub fn seqno(&self) -> SeqNo {
        self.1
    }

    pub fn encoded_len(&self) -> usize {
        4 + self.0.len() + 8
    }

    pub fn encode_to_bytes(&self) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        buf.freeze()
    }

    /// Encodes the Key into the provided buffer.
    pub fn encode_into(&self, buf: &mut bytes::BytesMut) {
        let key_len = self.0.len() as u32;
        buf.put_u32_le(key_len);
        buf.put_slice(&self.0);
        buf.put_u64_le(self.1.get());
    }

    pub fn decode_from(buf: &mut bytes::Bytes) -> anyhow::Result<Self> {
        let key_len = buf.try_get_u32_le()?;

        if buf.remaining() < key_len as usize + 8 {
            anyhow::bail!("Buffer too small to decode Key");
        }

        let user_key = buf.copy_to_bytes(key_len as usize);

        let seqno = buf.try_get_u64_le()?;

        Ok(Key(user_key, SeqNo(seqno)))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.0.cmp(&other.0) {
            std::cmp::Ordering::Equal => {}
            ord => return Some(ord),
        }

        Some(match self.1.cmp(&other.1) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
        })
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.partial_cmp(other) {
            Some(ord) => ord,
            None => unreachable!("PartialOrd impl never returns None"),
        }
    }
}
