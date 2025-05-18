use std::{collections::BTreeMap, io};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::{
    DecoderVersioned, Decoder, Encoder,
    messages::{ApiVersionsRequest, ApiVersionsResponse, MetadataRequest},
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

#[derive(Copy, Clone, Debug)]
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

    pub fn versions(&self) -> VersionRange {
        match self {
            ApiKey::Metadata => MetadataRequest::VERSIONS,
            ApiKey::ApiVersions => ApiVersionsRequest::VERSIONS,
        }
    }

    pub fn all() -> impl Iterator<Item = Self> {
        (0..i16::MAX).filter_map(|key| Self::try_from(key).ok())
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

#[derive(Debug)]
pub struct KafkaRequest {
    pub header: RequestHeader,
    pub request: RequestKind,
}

impl Decoder for KafkaRequest {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error> {
        let header = RequestHeader::decode(buf)?;
        let request = RequestKind::decode(buf, &header)?;

        Ok(KafkaRequest { header, request })
    }
}

#[derive(Debug)]
pub enum RequestKind {
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

impl RequestKind {
    fn decode(buf: &mut BytesMut, header: &RequestHeader) -> Result<Self, io::Error> {
        Ok(match header.api_key {
            ApiKey::Metadata => Self::Metadata(MetadataRequest::decode(buf, header.version)?),
            ApiKey::ApiVersions => {
                Self::ApiVersions(ApiVersionsRequest::decode(buf, header.version)?)
            }
        })
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Decoder for RequestHeader {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error> {
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

pub trait Request: Message + DecoderVersioned {
    type Response: Response;
}

pub trait Response: Message + Encoder {}

pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub response: ResponseKind,
}

impl Encoder for KafkaResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        self.header.encode(buf)?;
        self.response.encode(buf)?;
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

pub enum ResponseKind {
    ApiVersions(ApiVersionsResponse),
}

impl Encoder for ResponseKind {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        match self {
            ResponseKind::ApiVersions(response) => {
                response.encode(buf)?;
            }
        }
        Ok(())
    }
}
