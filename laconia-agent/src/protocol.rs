use std::io;

use bytes::BytesMut;

pub mod messages;
pub mod primitives;

pub trait Decoder: Sized {
    fn decode(buf: &mut BytesMut) -> Result<Self, io::Error>;
}

pub trait Decodable: Sized {
    fn decode(buf: &mut BytesMut, version: i16) -> Result<Self, io::Error>;
}
