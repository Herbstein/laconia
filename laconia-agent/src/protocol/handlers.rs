use std::{io, marker::PhantomData};

use async_trait::async_trait;
use bytes::BytesMut;

use crate::{
    ConnectionState, RequestHeader, VersionRange,
    protocol::{request::Request, response::AnyResponse},
};

mod api_versions;
pub use api_versions::ApiVersionsHandler;

mod metadata;
pub use metadata::MetadataHandler;

mod find_coordinator;
pub use find_coordinator::FindCoordinatorHandler;

pub trait RequestHandler<Req: Request>: Send + Sync {
    fn handle(
        &self,
        request: Req,
        state: &mut ConnectionState,
    ) -> impl Future<Output = Result<Req::Response, io::Error>> + Send;
}

#[async_trait]
pub(crate) trait AnyRequestHandler: Send + Sync {
    async fn handle(
        &self,
        buf: &mut BytesMut,
        header: &RequestHeader,
        state: &mut ConnectionState,
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

#[async_trait]
impl<Req, H> AnyRequestHandler for TypedRequestHandler<Req, H>
where
    H: RequestHandler<Req>,
    Req: Request,
    <Req as Request>::Response: 'static,
{
    async fn handle(
        &self,
        buf: &mut BytesMut,
        header: &RequestHeader,
        state: &mut ConnectionState,
    ) -> Result<Box<dyn AnyResponse>, io::Error> {
        let request = Req::decode(buf, header.version)?;
        let response = self.handler.handle(request, state).await?;
        Ok(Box::new(response))
    }

    fn header_version(&self, version: i16) -> i16 {
        Req::header_version(version)
    }

    fn versions(&self) -> VersionRange {
        Req::VERSIONS
    }
}
