use std::{collections::BTreeMap, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::{VarIntReader, VarIntWriter};
use uuid::Uuid;

use crate::protocol::{Decoder, DecoderVersioned, Encoder};

impl Decoder for bool {
    fn decode(buf: &mut BytesMut) -> Result<bool, io::Error> {
        let value = buf.get_u8();
        Ok(match value {
            0 => false,
            1 => true,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid bool value",
                ));
            }
        })
    }
}

impl Encoder for bool {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        if *self {
            buf.put_u8(1);
        } else {
            buf.put_u8(0);
        }
        Ok(())
    }
}

impl Decoder for i16 {
    fn decode(buf: &mut BytesMut) -> Result<i16, io::Error> {
        if buf.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data",
            ));
        }

        Ok(buf.get_i16())
    }
}

impl Encoder for i16 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i16(*self);
        Ok(())
    }
}

impl Encoder for i32 {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i32(*self);
        Ok(())
    }
}

impl Decoder for Uuid {
    fn decode(buf: &mut BytesMut) -> Result<Uuid, io::Error> {
        if buf.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data",
            ));
        }

        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes);
        Ok(Uuid::from_bytes(bytes))
    }
}

impl Decoder for String {
    fn decode(buf: &mut BytesMut) -> Result<String, io::Error> {
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for string length",
            ));
        }

        let len = i16::decode(buf)? as usize;

        if buf.len() < len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough bytes for string data",
            ));
        }

        let str_bytes = buf.split_to(len);
        let str = match String::from_utf8(str_bytes.to_vec()) {
            Ok(str) => str,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };

        Ok(str)
    }
}

pub struct NullableString(pub String);

impl Decoder for NullableString {
    fn decode(buf: &mut BytesMut) -> Result<NullableString, io::Error> {
        if buf.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for nullable string length",
            ));
        }

        let len = buf.get_i16();

        if len == -1 {
            return Ok(NullableString(String::new()));
        }

        if buf.len() < len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for nullable string data",
            ));
        }

        let str_bytes = buf.split_to(len as usize);
        let str = match String::from_utf8(str_bytes.to_vec()) {
            Ok(str) => str,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };

        Ok(NullableString(str))
    }
}

pub struct CompactString(pub String);

impl Decoder for CompactString {
    fn decode(buf: &mut BytesMut) -> Result<CompactString, io::Error> {
        let length = buf.reader().read_varint::<u32>()? as usize;

        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-length compact string",
            ));
        }

        let length = length - 1;

        if buf.len() < length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough bytes for compact string data",
            ));
        }

        let str_bytes = buf.split_to(length);
        let str = match String::from_utf8(str_bytes.to_vec()) {
            Ok(str) => str,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };

        Ok(Self(str))
    }
}

impl Encoder for CompactString {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        let bytes = self.0.as_bytes();
        buf.writer().write_varint(bytes.len() as u32 + 1)?;
        buf.put_slice(bytes);
        Ok(())
    }
}

pub struct CompactNullableString(pub String);

impl Decoder for CompactNullableString {
    fn decode(buf: &mut BytesMut) -> Result<CompactNullableString, io::Error> {
        let length = buf.reader().read_varint::<u32>()? as usize;

        if length == 0 {
            return Ok(Self(String::new()));
        }

        let length = length - 1;

        if buf.len() < length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough bytes for string data",
            ));
        }

        let str_bytes = buf.split_to(length);
        let str = match String::from_utf8(str_bytes.to_vec()) {
            Ok(str) => str,
            Err(err) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, err));
            }
        };

        Ok(Self(str))
    }
}

impl Encoder for CompactNullableString {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        let bytes = self.0.as_bytes();
        if bytes.len() == 0 {
            buf.writer().write_varint(0u32)?;
        } else {
            buf.writer().write_varint(bytes.len() as u32 + 1)?;
            buf.put_slice(bytes);
        }
        Ok(())
    }
}

impl<T> Decoder for Vec<T>
where
    T: Decoder,
{
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error> {
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for array length",
            ));
        }

        let length = buf.get_u32() as usize;

        let mut array = Vec::with_capacity(length);
        for _ in 0..length {
            array.push(T::decode(buf)?);
        }

        Ok(array)
    }
}

impl<T> DecoderVersioned for Vec<T>
where
    T: DecoderVersioned,
{
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        if buf.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for array length",
            ));
        }

        let length = buf.get_u32() as usize;

        let mut array = Vec::with_capacity(length);
        for _ in 0..length {
            array.push(T::decode(buf, version)?);
        }

        Ok(array)
    }
}

impl<T> Encoder for Vec<T>
where
    T: Encoder,
{
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i32(self.len() as i32);
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }
}

pub struct CompactArray<T>(pub Vec<T>);

impl<T> Decoder for CompactArray<T>
where
    T: Decoder,
{
    fn decode(buf: &mut BytesMut) -> Result<CompactArray<T>, io::Error> {
        let length = buf.reader().read_varint::<u32>()? as usize;

        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compact array length is 0",
            ));
        }

        let length = length - 1;
        let mut array = Vec::with_capacity(length);
        for _ in 0..length {
            array.push(T::decode(buf)?);
        }

        Ok(Self(array))
    }
}

impl<T> DecoderVersioned for CompactArray<T>
where
    T: DecoderVersioned,
{
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        let length = buf.reader().read_varint::<u32>()? as usize;

        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compact array length is 0",
            ));
        }

        let length = length - 1;
        let mut array = Vec::with_capacity(length);
        for _ in 0..length {
            array.push(T::decode(buf, version)?);
        }

        Ok(Self(array))
    }
}

impl<T> Encoder for CompactArray<T>
where
    T: Encoder,
{
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.writer().write_varint((self.0.len() + 1) as u32)?;
        for element in &self.0 {
            element.encode(buf)?;
        }

        Ok(())
    }
}

impl Decoder for BTreeMap<i32, Bytes> {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error> {
        let mut tagged_fields = BTreeMap::new();
        let num_tagged_fields = buf.reader().read_varint::<u32>()? as usize;
        for _ in 0..num_tagged_fields {
            let tag = buf.reader().read_varint::<u32>()?;
            let size = buf.reader().read_varint::<u32>()? as usize;
            let unknown_value = buf.split_to(size);
            tagged_fields.insert(tag as i32, unknown_value.freeze());
        }
        Ok(tagged_fields)
    }
}

impl Encoder for BTreeMap<i32, Bytes> {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        if !self.is_empty() {
            panic!("cannot send non-empty tagged fields")
        }

        buf.writer().write_varint(0u32)?;
        // TODO(herbstein): actually write fields
        Ok(())
    }
}
