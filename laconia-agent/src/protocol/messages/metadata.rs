use std::{collections::BTreeMap, io};

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::{
    Message, VersionRange,
    protocol::{
        Decoder, DecoderVersioned, Encoder, EncoderVersioned,
        primitives::{CompactArray, CompactArrayRef, CompactNullableString, CompactString},
        request::Request,
        response::Response,
    },
};

#[derive(Debug)]
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

impl Request for MetadataRequest {
    type Response = MetadataResponse;
}

impl DecoderVersioned for MetadataRequest {
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

#[derive(Debug)]
pub struct MetadataRequestTopic {
    pub topic_id: Uuid,
    pub name: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl DecoderVersioned for MetadataRequestTopic {
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
            CompactNullableString::decode(buf)?.0
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

pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<MetadataResponseBrokers>,
    pub cluster_id: String,
    pub controller_id: i32,
    pub topics: Vec<MetadataResponseTopic>,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl EncoderVersioned for MetadataResponse {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        self.throttle_time_ms.encode(buf)?;
        CompactArrayRef(&self.brokers).encode(buf, version)?;
        CompactNullableString(self.cluster_id.clone()).encode(buf)?;
        self.controller_id.encode(buf)?;
        CompactArrayRef(&self.topics).encode(buf, version)?;
        self.tagged_fields.encode(buf)?;
        Ok(())
    }
}

impl Response for MetadataResponse {}

#[derive(Clone)]
pub struct MetadataResponseBrokers {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl EncoderVersioned for MetadataResponseBrokers {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        self.node_id.encode(buf)?;
        CompactString(self.host.clone()).encode(buf)?;
        self.port.encode(buf)?;
        CompactNullableString(self.rack.clone()).encode(buf)?;
        self.tagged_fields.encode(buf)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MetadataResponseTopic {
    pub error_code: i16,
    pub name: String,
    pub topic_id: Uuid,
    pub is_internal: bool,
    pub partitions: Vec<MetadataResponseTopicPartition>,
    pub topic_authorized_operations: i32,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl EncoderVersioned for MetadataResponseTopic {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        self.error_code.encode(buf)?;
        CompactString(self.name.clone()).encode(buf)?;
        self.is_internal.encode(buf)?;
        CompactArrayRef(&self.partitions).encode(buf, version)?;
        self.topic_authorized_operations.encode(buf)?;
        self.tagged_fields.encode(buf)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MetadataResponseTopicPartition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    pub offline_replicas: Vec<i32>,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl EncoderVersioned for MetadataResponseTopicPartition {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        buf.put_i16(self.error_code);
        buf.put_i32(self.partition_index);
        buf.put_i32(self.leader_id);
        buf.put_i32(self.leader_epoch);
        CompactArrayRef(&self.replica_nodes).encode(buf)?;
        CompactArrayRef(&self.isr_nodes).encode(buf)?;
        CompactArrayRef(&self.offline_replicas).encode(buf)?;
        self.tagged_fields.encode(buf)?;
        Ok(())
    }
}
