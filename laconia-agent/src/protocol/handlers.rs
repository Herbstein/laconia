use std::{io, marker::PhantomData};

use bytes::BytesMut;

use crate::{
    RequestHeader, VersionRange,
    protocol::{request::Request, response::AnyResponse},
};

pub mod api_versions;
pub mod metadata;

pub trait RequestHandler<Req: Request> {
    fn handle(&self, request: Req) -> Result<Req::Response, io::Error>;
}

pub(crate) trait AnyRequestHandler: Send + Sync {
    fn handle(
        &self,
        buf: &mut BytesMut,
        header: &RequestHeader,
    ) -> Result<Box<dyn AnyResponse>, io::Error>;

    fn header_version(&self, version: i16) -> i16;

    fn versions(&self) -> VersionRange;
}

pub(crate) struct TypedRequestHandler<Req: Request, H: RequestHandler<Req>> {
    handler: H,
    _phantom: PhantomData<Req>,
}

impl<Req: Request, H: RequestHandler<Req>> TypedRequestHandler<Req, H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<Req, H> AnyRequestHandler for TypedRequestHandler<Req, H>
where
    H: RequestHandler<Req> + Send + Sync,
    Req: Request + Send + Sync,
    <Req as Request>::Response: 'static,
{
    fn handle(
        &self,
        buf: &mut BytesMut,
        header: &RequestHeader,
    ) -> Result<Box<dyn AnyResponse>, io::Error> {
        let request = Req::decode(buf, header.version)?;
        let response = self.handler.handle(request)?;
        Ok(Box::new(response))
    }

    fn header_version(&self, version: i16) -> i16 {
        Req::header_version(version)
    }

    fn versions(&self) -> VersionRange {
        Req::VERSIONS
    }
}
