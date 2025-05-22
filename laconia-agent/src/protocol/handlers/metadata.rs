use std::io;

use crate::protocol::{
    handlers::RequestHandler,
    messages::{MetadataRequest, MetadataResponse},
};

pub struct MetadataRequestHandler;

impl RequestHandler<MetadataRequest> for MetadataRequestHandler {
    fn handle(&self, request: MetadataRequest) -> Result<MetadataResponse, io::Error> {
        Ok(MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![],
            cluster_id: "".to_string(),
            controller_id: 0,
            topics: vec![],
            tagged_fields: Default::default(),
        })
    }
}
