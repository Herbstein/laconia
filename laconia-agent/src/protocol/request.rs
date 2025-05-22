use crate::{
    Message,
    protocol::{DecoderVersioned, response::Response},
};

pub trait Request: Message + DecoderVersioned + Send + Sync {
    type Response: Response;
}
