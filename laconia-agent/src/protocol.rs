use std::io;

use bytes::BytesMut;

pub mod handlers;
pub mod messages;
pub mod primitives;
pub mod registry;
pub mod request;
pub mod response;

pub trait Encoder {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error>;
}

pub trait EncoderVersioned {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error>;
}

pub trait Decoder: Sized {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error>;
}

pub trait DecoderVersioned: Sized {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error>;
}
