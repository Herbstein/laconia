use std::{collections::BTreeMap, io};

use bytes::{Buf, Bytes, BytesMut};
use integer_encoding::VarIntReader;

use crate::protocol::{
    Decodable, Decoder,
    messages::{ApiVersionsRequest, MetadataRequest},
    primitives::NullableString,
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

        let len = u32::from_be_bytes(src[..4].try_into().unwrap()) as usize;
        if src.len() - 4 < len {
            return Ok(None);
        }

        src.advance(4);
        Ok(Some(src.split_to(len).freeze()))
    }
}

pub(crate) trait Encoder {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error>;
}

pub enum ApiKey {
    Metadata = 3,
    ApiVersions = 18,
}

impl ApiKey {
    fn request_header_version(&self, version: i16) -> i16 {
        match self {
            ApiKey::Metadata => MetadataRequest::header_version(version),
            ApiKey::ApiVersions => ApiVersionsRequest::header_version(version),
        }
    }

    fn valid_versions(&self) -> VersionRange {
        match self {
            ApiKey::Metadata => MetadataRequest::VERSIONS,
            ApiKey::ApiVersions => ApiVersionsRequest::VERSIONS,
        }
    }

    fn all() -> Vec<ApiKey> {
        (0..i16::MAX)
            .filter_map(|key| Self::try_from(key).ok())
            .collect()
    }
}

impl TryFrom<i16> for ApiKey {
    type Error = i16;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        Ok(match value {
            3 => ApiKey::Metadata,
            18 => ApiKey::ApiVersions,
            _ => return Err(value),
        })
    }
}

pub enum RequestKind {
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

impl Decoder for RequestKind {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error> {
        todo!()
    }
}

pub struct KafkaHeader {
    pub key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tagged_fields: BTreeMap<u32, Bytes>,
}

impl Decoder for KafkaHeader {
    fn decode(buf: &mut BytesMut) -> Result<KafkaHeader, io::Error> {
        if buf.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for v1 kafka header",
            ));
        }

        let key = buf.get_i16();
        let version = buf.get_i16();
        let correlation_id = buf.get_i32();

        let api_key = match ApiKey::try_from(key) {
            Ok(api_key) => api_key,
            Err(key) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid api key: {}", key),
                ));
            }
        };

        let client_id = NullableString::decode(buf)?.0;

        let header_version = api_key.request_header_version(version);

        let mut tagged_fields = BTreeMap::new();
        if header_version > 1 {
            let num_tagged_fields = buf.reader().read_varint::<u32>()? as usize;
            for _ in 0..num_tagged_fields {
                let tag = buf.reader().read_varint::<u32>()?;
                let size = buf.reader().read_varint::<u32>()? as usize;
                let unknown_value = buf.split_to(size).freeze();
                tagged_fields.insert(tag, unknown_value);
            }
        }

        Ok(Self {
            key,
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

pub trait Encodable: Sized {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error>;

    fn compute_size(&self, version: i16) -> Result<usize, io::Error>;
}

pub trait Request: Message + Decodable {
    const KEY: u16;

    type Response: Message + Encodable;
}
