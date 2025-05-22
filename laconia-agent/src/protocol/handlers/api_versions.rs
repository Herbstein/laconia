use std::io;

use crate::{
    ConnectionState,
    protocol::{
        handlers::RequestHandler,
        messages::{ApiVersionsApiKeys, ApiVersionsRequest, ApiVersionsResponse},
    },
};

pub struct ApiVersionsHandler;

impl RequestHandler<ApiVersionsRequest> for ApiVersionsHandler {
    async fn handle(
        &self,
        _request: ApiVersionsRequest,
        state: &mut ConnectionState,
    ) -> Result<ApiVersionsResponse, io::Error> {
        println!("Handling ApiVersionsRequest");

        let api_versions = state.registry.all_api_keys().map(|key| {
            let versions = state.registry.versions(key).expect("registry returned key");
            ApiVersionsApiKeys {
                api_key: key,
                min_version: versions.min,
                max_version: versions.max,
                tagged_fields: Default::default(),
            }
        });

        Ok(ApiVersionsResponse {
            error_code: 0,
            api_keys: api_versions.collect(),
            throttle_time_ms: 0,
            tagged_fields: Default::default(),
        })
    }
}
