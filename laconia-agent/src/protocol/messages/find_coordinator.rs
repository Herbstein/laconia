use std::io;

use bytes::BytesMut;

use crate::{
    Message, VersionRange,
    protocol::{DecoderVersioned, EncoderVersioned, request::Request, response::Response},
};

pub struct FindCoordinatorRequest {}

impl Message for FindCoordinatorRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 6 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;

    fn header_version(version: i16) -> i16 {
        if version > 2 { 2 } else { 1 }
    }
}

impl Request for FindCoordinatorRequest {
    type Response = FindCoordinatorResponse;
}

impl DecoderVersioned for FindCoordinatorRequest {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error> {
        todo!()
    }
}

pub struct FindCoordinatorResponse {}

impl Response for FindCoordinatorResponse {}

impl EncoderVersioned for FindCoordinatorResponse {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        todo!()
    }
}
