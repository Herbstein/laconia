use std::io;

use crate::{
    ConnectionState,
    protocol::{
        handlers::RequestHandler,
        messages::{MetadataRequest, MetadataResponse},
    },
};

pub struct MetadataRequestHandler;

impl RequestHandler<MetadataRequest> for MetadataRequestHandler {
    async fn handle(
        &self,
        request: MetadataRequest,
        state: &mut ConnectionState,
    ) -> Result<MetadataResponse, io::Error> {
        println!("Handling MetadataRequest");
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
