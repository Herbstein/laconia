use std::{collections::BTreeMap, io};

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    Message, VersionRange,
    protocol::{
        Decoder, DecoderVersioned, Encoder,
        primitives::{CompactArray, CompactString},
        request::Request,
        response::Response,
    },
};

#[derive(Debug)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Message for ApiVersionsRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 4 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;

    fn header_version(version: i16) -> i16 {
        if version < 3 { 1 } else { 2 }
    }
}

impl DecoderVersioned for ApiVersionsRequest {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        let client_software_name = if version < 3 {
            String::new()
        } else {
            CompactString::decode(buf)?.0
        };

        let client_software_version = if version < 3 {
            String::new()
        } else {
            CompactString::decode(buf)?.0
        };

        let mut tagged_fields = BTreeMap::new();
        if version > 2 {
            tagged_fields = Decoder::decode(buf)?;
        }

        Ok(Self {
            client_software_name,
            client_software_version,
            tagged_fields,
        })
    }
}

impl Request for ApiVersionsRequest {
    type Response = ApiVersionsResponse;
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersionsApiKeys>,
    pub throttle_time_ms: i32,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Encoder for ApiVersionsResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i16(self.error_code);
        CompactArray(self.api_keys.clone()).encode(buf)?;
        buf.put_i32(self.throttle_time_ms);
        self.tagged_fields.encode(buf)?;

        Ok(())
    }
}

impl Response for ApiVersionsResponse {}

#[derive(Clone)]
pub struct ApiVersionsApiKeys {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Encoder for ApiVersionsApiKeys {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.put_i16(self.api_key as i16);
        buf.put_i16(self.min_version);
        buf.put_i16(self.max_version);
        self.tagged_fields.encode(buf)?;

        Ok(())
    }
}
