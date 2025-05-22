use std::{collections::BTreeMap, io};

use bytes::BytesMut;

use crate::{
    RequestHeader, VersionRange,
    protocol::{
        handlers::{AnyRequestHandler, RequestHandler, TypedRequestHandler},
        request::Request,
        response::AnyResponse,
    },
};

pub struct MessageRegistry {
    handlers: BTreeMap<i16, Box<dyn AnyRequestHandler>>,
}

impl MessageRegistry {
    pub fn new() -> Self {
        Self {
            handlers: BTreeMap::new(),
        }
    }

    pub fn register<Req, H>(&mut self, key: i16, handler: H)
    where
        Req: Request + Send + Sync + 'static,
        H: RequestHandler<Req> + Send + Sync + 'static,
    {
        self.handlers
            .insert(key, Box::new(TypedRequestHandler::new(handler)));
    }

    pub async fn handle_request(
        &self,
        buf: &mut BytesMut,
        header: &RequestHeader,
    ) -> Result<Box<dyn AnyResponse>, io::Error> {
        match self.handlers.get(&header.api_key) {
            Some(handler) => handler.handle(buf, header).await,
            None => Err(io::Error::other(format!(
                "Unsupported api key: {}",
                header.api_key
            ))),
        }
    }

    pub fn header_version(&self, api_key: i16, version: i16) -> Result<i16, io::Error> {
        match self.handlers.get(&api_key) {
            Some(handler) => Ok(handler.header_version(version)),
            None => Err(io::Error::other(format!(
                "unsupported api key: {}",
                api_key
            ))),
        }
    }

    pub fn versions(&self, api_key: i16) -> Result<VersionRange, io::Error> {
        match self.handlers.get(&api_key) {
            Some(handler) => Ok(handler.versions()),
            None => Err(io::Error::other(format!(
                "unsupported api key: {}",
                api_key,
            ))),
        }
    }

    pub fn all_api_keys(&self) -> impl Iterator<Item = i16> {
        self.handlers.keys().copied()
    }
}
