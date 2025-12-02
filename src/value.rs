use bytes::{Buf, BufMut};

#[derive(Debug, Clone)]
#[repr(u8)]
pub enum ValueType {
    Data = 0,
    Tombstone = 1,
}

impl ValueType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            x if x == ValueType::Data as u8 => Some(ValueType::Data),
            x if x == ValueType::Tombstone as u8 => Some(ValueType::Tombstone),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Data(bytes::Bytes),
    Tombstone,
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Data(_) => ValueType::Data,
            Value::Tombstone => ValueType::Tombstone,
        }
    }

    pub fn encode_into(&self, buf: &mut bytes::BytesMut) {
        buf.put_u8(self.value_type() as u8);

        match self {
            Value::Data(data) => {
                buf.put_u32_le(data.len() as u32);
                buf.put_slice(data);
            }
            Value::Tombstone => {}
        }
    }

    pub fn decode_from(buf: &mut bytes::Bytes) -> anyhow::Result<Self> {
        let value_type = ValueType::from_u8(buf.try_get_u8()?)
            .ok_or_else(|| anyhow::anyhow!("Invalid value type"))?;

        match value_type {
            ValueType::Data => {
                let len = buf.try_get_u32_le()?;

                if buf.remaining() < len as usize {
                    return Err(anyhow::anyhow!(
                        "Buffer underflow while decoding Value::Data"
                    ));
                }

                let data = buf.copy_to_bytes(len as usize);

                Ok(Value::Data(data))
            }
            ValueType::Tombstone => Ok(Value::Tombstone),
        }
    }
}
