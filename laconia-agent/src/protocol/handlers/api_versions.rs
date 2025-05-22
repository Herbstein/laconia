use std::io;

use crate::protocol::{
    handlers::RequestHandler,
    messages::{ApiVersionsApiKeys, ApiVersionsRequest, ApiVersionsResponse},
    registry::MessageRegistry,
};

pub struct ApiVersionsHandler {
    api_versions: Vec<ApiVersionsApiKeys>,
}

impl ApiVersionsHandler {
    pub fn new(registry: &MessageRegistry) -> Self {
        let versions = registry.all_api_keys().map(|key| {
            let versions = registry.versions(key).expect("registry returned key");
            ApiVersionsApiKeys {
                api_key: key,
                min_version: versions.min,
                max_version: versions.max,
                tagged_fields: Default::default(),
            }
        });

        Self {
            api_versions: versions.collect(),
        }
    }
}

impl RequestHandler<ApiVersionsRequest> for ApiVersionsHandler {
    async fn handle(&self, _request: ApiVersionsRequest) -> Result<ApiVersionsResponse, io::Error> {
        println!("Handling ApiVersionsRequest");
        Ok(ApiVersionsResponse {
            error_code: 0,
            api_keys: self.api_versions.clone(),
            throttle_time_ms: 0,
            tagged_fields: Default::default(),
        })
    }
}
