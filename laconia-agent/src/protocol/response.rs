use std::io;

use bytes::BytesMut;

use crate::protocol::EncoderVersioned;

pub trait Response: EncoderVersioned + Send {}

pub trait AnyResponse: Send {
    fn encode_any(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error>;
}

impl<T: Response> AnyResponse for T {
    fn encode_any(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        self.encode(buf, version)
    }
}
