use std::{collections::BTreeMap, io};

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crate::{
    Message, VersionRange,
    protocol::{
        Decodable, Decoder,
        primitives::{CompactArray, CompactString, NullableCompactString},
    },
};

pub struct MetadataRequest {
    pub topics: Vec<MetadataRequestTopic>,
    pub allow_auto_topic_creation: bool,
    pub include_cluster_authorized_operations: bool,
    pub include_topic_authorized_operations: bool,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Message for MetadataRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 13 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;

    fn header_version(version: i16) -> i16 {
        if version < 9 { 1 } else { 2 }
    }
}

impl Decodable for MetadataRequest {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        if !Self::VERSIONS.contains(version) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid version",
            ));
        }

        let topics = if version < 9 {
            Vec::<MetadataRequestTopic>::decode(buf, version)?
        } else {
            CompactArray::<MetadataRequestTopic>::decode(buf, version)?.0
        };

        let allow_auto_topic_creation = if version < 4 {
            false
        } else {
            bool::decode(buf)?
        };

        let include_cluster_authorized_operations = if version < 8 {
            false
        } else if version < 11 {
            bool::decode(buf)?
        } else {
            false
        };

        let include_topic_authorized_operations = if version < 8 {
            false
        } else {
            bool::decode(buf)?
        };

        let mut tagged_fields = BTreeMap::new();
        if version > 8 {
            tagged_fields = Decoder::decode(buf)?;
        }

        Ok(Self {
            topics,
            allow_auto_topic_creation,
            include_cluster_authorized_operations,
            include_topic_authorized_operations,
            tagged_fields,
        })
    }
}

pub struct MetadataRequestTopic {
    pub topic_id: Uuid,
    pub name: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl Decodable for MetadataRequestTopic {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        let topic_id = if version > 9 {
            Uuid::decode(buf)?
        } else {
            Uuid::nil()
        };

        let name = if version < 9 {
            String::decode(buf)?
        } else if version < 10 {
            CompactString::decode(buf)?.0
        } else {
            NullableCompactString::decode(buf)?.0
        };

        let mut tagged_fields = BTreeMap::new();
        if version > 8 {
            tagged_fields = Decoder::decode(buf)?;
        };

        Ok(Self {
            topic_id,
            name,
            tagged_fields,
        })
    }
}
