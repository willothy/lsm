//! This modle implements a generic on-disk log structure with framing around postcard.

pub fn write_framed<W, T>(mut writer: W, data: &T) -> postcard::Result<usize>
where
    W: std::io::Write,
    T: serde::Serialize,
{
    let bytes = postcard::to_stdvec(&data)?;

    let len: u32 = bytes.len().try_into().expect("Length exceeds u32::MAX");

    postcard::to_io(&len, &mut writer)?;
    writer
        .write_all(&bytes)
        .expect("Failed to write framed data");

    Ok(bytes.len() + 4)
}

pub fn read_framed<R, T>(mut reader: R) -> postcard::Result<T>
where
    R: std::io::Read,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    let len: u32 = postcard::from_io((&mut reader, &mut len_buf))?.0;

    let mut buf = vec![0u8; len as usize];

    postcard::from_io((&mut reader, &mut buf)).map(|(data, _)| data)
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
