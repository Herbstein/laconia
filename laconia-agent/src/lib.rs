use std::{collections::BTreeMap, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::{
    Decoder, Encoder, primitives::NullableString, registry::MessageRegistry, response::AnyResponse,
};

pub mod protocol;

pub struct KafkaMessageCodec;

impl tokio_util::codec::Decoder for KafkaMessageCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = i32::from_be_bytes(src[..4].try_into().unwrap()) as usize;
        if src.len() - 4 < len {
            return Ok(None);
        }

        src.advance(4);
        Ok(Some(src.split_to(len).freeze()))
    }
}

impl<T> tokio_util::codec::Encoder<T> for KafkaMessageCodec
where
    T: Encoder,
{
    type Error = io::Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::new();
        item.encode(&mut buf)?;

        dst.put_i32(buf.len() as i32);
        dst.put(buf);

        Ok(())
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub response: Box<dyn AnyResponse>,
}

impl KafkaRequest {
    pub fn decode_and_handle(
        buf: &mut BytesMut,
        registry: &MessageRegistry,
    ) -> Result<Self, io::Error> {
        let header = RequestHeader::decode(buf, registry)?;
        let response = registry.handle_request(buf, &header)?;
        Ok(Self { header, response })
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl RequestHeader {
    fn decode(buf: &mut BytesMut, registry: &MessageRegistry) -> Result<Self, io::Error> {
        if buf.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for v1 kafka header",
            ));
        }

        let api_key = buf.get_i16();
        let version = buf.get_i16();
        let correlation_id = buf.get_i32();

        registry.versions(api_key)?;

        let client_id = NullableString::decode(buf)?.0;

        let header_version = registry.header_version(api_key, version)?;

        let mut tagged_fields = BTreeMap::new();
        if header_version > 1 {
            tagged_fields = Decoder::decode(buf)?;
        }

        Ok(Self {
            api_key,
            version,
            correlation_id,
            client_id,
            tagged_fields,
        })
    }
}

pub struct VersionRange {
    pub min: i16,
    pub max: i16,
}

impl VersionRange {
    pub fn contains(&self, version: i16) -> bool {
        self.min <= version && version <= self.max
    }
}

pub trait Message: Sized {
    const VERSIONS: VersionRange;
    const DEPRECATED_VERSIONS: Option<VersionRange>;

    fn header_version(version: i16) -> i16;
}

pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub response: Box<dyn AnyResponse>,
}

impl KafkaResponse {
    pub fn new(header: &RequestHeader, response: Box<dyn AnyResponse>) -> Self {
        Self {
            header: ResponseHeader {
                correlation_id: header.correlation_id,
            },
            response,
        }
    }
}

impl Encoder for KafkaResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        self.header.encode(buf)?;
        self.response.encode_any(buf)?;
        Ok(())
    }
}

pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl Encoder for ResponseHeader {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i32(self.correlation_id);
        Ok(())
    }
}
