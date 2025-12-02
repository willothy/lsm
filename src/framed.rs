//! This modle implements a generic on-disk log structure with framing around postcard.

use std::io::Write;

use anyhow::Context;

pub fn write_framed<W, T>(mut writer: W, data: &T) -> anyhow::Result<usize>
where
    W: Write,
    T: serde::Serialize,
{
    let bytes = postcard::to_stdvec(&data)?;

    let len: u32 = bytes.len().try_into().context("Length exceeds u32::MAX")?;

    writer
        .write_all(&len.to_le_bytes())
        .context("Failed to write framed length")?;
    writer
        .write_all(&bytes)
        .context("Failed to write framed data")?;

    Ok(bytes.len() + 4)
}

pub fn read_framed<R, T>(mut reader: R) -> postcard::Result<T>
where
    R: std::io::Read,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .map_err(|_| postcard::Error::DeserializeUnexpectedEnd)?;

    let len = u32::from_le_bytes(len_buf);

    if len == 0 {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }

    let mut buf = vec![0u8; len as usize];

    reader
        .read_exact(&mut buf)
        .map_err(|_| postcard::Error::DeserializeUnexpectedEnd)?;

    postcard::from_bytes(&buf)
}

pub fn read_all_framed<R, T>(mut reader: R) -> postcard::Result<Vec<T>>
where
    R: std::io::Read,
    T: serde::de::DeserializeOwned,
{
    let mut res = Vec::new();

    loop {
        match read_framed::<_, T>(&mut reader) {
            Ok(record) => res.push(record),
            Err(e) => match e {
                postcard::Error::DeserializeUnexpectedEnd => {
                    break;
                }
                e => return Err(e),
            },
        };
    }

    Ok(res)
}
