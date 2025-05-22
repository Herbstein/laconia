use std::io;

use bytes::BytesMut;

use crate::{ protocol::Encoder};

pub trait Response: Encoder + Send {}

pub trait AnyResponse: Send {
    fn encode_any(&self, buf: &mut BytesMut) -> Result<(), io::Error>;
}

impl<T: Response> AnyResponse for T {
    fn encode_any(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        self.encode(buf)
    }
}
