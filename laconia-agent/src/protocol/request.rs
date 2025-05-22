use crate::{
    Message,
    protocol::{DecoderVersioned, response::Response},
};

pub trait Request: Message + DecoderVersioned {
    type Response: Response;
}
