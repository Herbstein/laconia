use std::{collections::BTreeMap, io};

use bytes::{Bytes, BytesMut};

use crate::{
    Message, VersionRange,
    protocol::{
        Decodable, Decoder,
        primitives::{CompactString, TaggedFields},
    },
};

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

impl Decodable for ApiVersionsRequest {
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
            tagged_fields = TaggedFields::decode(buf)?.0;
        }

        Ok(Self {
            client_software_name,
            client_software_version,
            tagged_fields,
        })
    }
}
