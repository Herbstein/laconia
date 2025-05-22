use std::io;

use crate::{
    ConnectionState,
    protocol::{
        handlers::RequestHandler,
        messages::{FindCoordinatorRequest, FindCoordinatorResponse},
    },
};

pub struct FindCoordinatorHandler;

impl RequestHandler<FindCoordinatorRequest> for FindCoordinatorHandler {
    async fn handle(
        &self,
        request: FindCoordinatorRequest,
        state: &mut ConnectionState,
    ) -> Result<FindCoordinatorResponse, io::Error> {
        unimplemented!();
    }
}
