use std::{collections::BTreeMap, io, sync::Arc};

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use futures::{SinkExt, StreamExt};
use laconia_liveness::liveness::{CheckinRequest, liveness_client::LivenessClient};
use serde::Deserialize;
use tokio::{
    net::{TcpListener, ToSocketAddrs},
    time,
};
use tokio_util::codec::Decoder as _;
use uuid::Uuid;

use crate::protocol::{
    Decoder, Encoder, EncoderVersioned,
    handlers::{ApiVersionsHandler, FindCoordinatorHandler, MetadataHandler},
    primitives::NullableString,
    registry::MessageRegistry,
    response::AnyResponse,
};

mod protocol;

pub struct KafkaMessageCodec;

impl tokio_util::codec::Decoder for KafkaMessageCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = i32::from_be_bytes(src[..4].try_into().unwrap()) as usize;
        if src.len() - 4 < len {
            return Ok(None);
        }

        src.advance(4);
        Ok(Some(src.split_to(len).freeze()))
    }
}

impl tokio_util::codec::Encoder<KafkaResponse> for KafkaMessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: KafkaResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut buf = BytesMut::new();
        item.encode(&mut buf)?;

        dst.put_i32(buf.len() as i32);
        dst.put(buf);

        Ok(())
    }
}

pub struct ConnectionState {
    pub(crate) registry: Arc<MessageRegistry>,
}

impl ConnectionState {
    pub fn new(registry: Arc<MessageRegistry>) -> Self {
        Self { registry }
    }
}

pub struct KafkaRequest {
    pub header: RequestHeader,
    pub response: Box<dyn AnyResponse>,
}

impl KafkaRequest {
    pub async fn decode_and_handle(
        buf: &mut BytesMut,
        registry: &MessageRegistry,
        state: &mut ConnectionState,
    ) -> Result<Self, io::Error> {
        let header = RequestHeader::decode(buf, registry)?;
        let response = registry.handle_request(buf, &header, state).await?;
        Ok(Self { header, response })
    }
}

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: i16,
    pub version: i16,
    pub correlation_id: i32,
    pub client_id: String,
    pub tagged_fields: BTreeMap<i32, Bytes>,
}

impl RequestHeader {
    fn decode(buf: &mut BytesMut, registry: &MessageRegistry) -> Result<Self, io::Error> {
        if buf.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not enough data for v1 kafka header",
            ));
        }

        let api_key = buf.get_i16();
        let version = buf.get_i16();
        let correlation_id = buf.get_i32();

        registry.versions(api_key)?;

        let client_id = NullableString::decode(buf)?.0;

        let header_version = registry.header_version(api_key, version)?;

        let mut tagged_fields = BTreeMap::new();
        if header_version > 1 {
            tagged_fields = Decoder::decode(buf)?;
        }

        Ok(Self {
            api_key,
            version,
            correlation_id,
            client_id,
            tagged_fields,
        })
    }
}

pub struct VersionRange {
    pub min: i16,
    pub max: i16,
}

impl VersionRange {
    pub fn new(min: i16, max: i16) -> Self {
        Self { min, max }
    }

    pub fn contains(&self, version: i16) -> bool {
        self.min <= version && version <= self.max
    }
}

pub trait Message: Sized {
    const VERSIONS: VersionRange;
    const DEPRECATED_VERSIONS: Option<VersionRange>;

    fn header_version(version: i16) -> i16;
}

pub struct KafkaResponse {
    pub header: ResponseHeader,
    pub response: Box<dyn AnyResponse>,
}

impl KafkaResponse {
    pub fn new(header: &RequestHeader, response: Box<dyn AnyResponse>) -> Self {
        Self {
            header: ResponseHeader {
                correlation_id: header.correlation_id,
            },
            response,
        }
    }
}

impl Encoder for KafkaResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), io::Error> {
        self.header.encode(buf, 1)?; // TODO(herbstein): determine header version
        self.response.encode_any(buf, i16::MAX)?; // TODO(herbstein): determine response version
        Ok(())
    }
}

pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl EncoderVersioned for ResponseHeader {
    fn encode(&self, buf: &mut BytesMut, version: i16) -> Result<(), io::Error> {
        buf.put_i32(self.correlation_id);
        Ok(())
    }
}

#[derive(Deserialize)]
struct Config {
    controlplane: String,
}

impl Config {
    fn from_figment() -> Result<Self> {
        let figment = Figment::new()
            .merge(Toml::file("config.toml"))
            .merge(Env::prefixed("LACONIA"));

        let config = figment.extract()?;

        Ok(config)
    }
}

struct KafkaServer {
    registry: Arc<MessageRegistry>,
    listener: TcpListener,
}

impl KafkaServer {
    async fn build(addr: impl ToSocketAddrs) -> Self {
        let mut registry = MessageRegistry::new();
        registry.register(3, MetadataHandler);
        registry.register(10, FindCoordinatorHandler);
        registry.register(18, ApiVersionsHandler);

        let registry = Arc::new(registry);

        let listener = TcpListener::bind(addr).await.unwrap();

        Self { registry, listener }
    }

    async fn accept(&self) -> Result<()> {
        let (stream, _) = self.listener.accept().await?;

        let registry = self.registry.clone();
        let mut connection_state = ConnectionState::new(registry.clone());

        let mut stream = KafkaMessageCodec.framed(stream);

        tokio::spawn(async move {
            while let Some(message) = stream.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        eprintln!("Kafka protocol error: {}", err);
                        break;
                    }
                };

                let mut message = BytesMut::from(message);

                let request =
                    KafkaRequest::decode_and_handle(&mut message, &registry, &mut connection_state)
                        .await
                        .unwrap();

                let response = KafkaResponse::new(&request.header, request.response);

                stream.send(response).await.unwrap();
            }
        });

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_figment()?;

    let kafka_server = KafkaServer::build("[::1]:8080").await;

    let id = Uuid::new_v4();

    let mut liveness_client = LivenessClient::connect(config.controlplane).await?;

    let checkin_reply = liveness_client
        .checkin(CheckinRequest { id: id.to_string() })
        .await?;

    let interval = checkin_reply.get_ref().interval;

    println!("checkin interval: {:?}", interval);

    loop {
        tokio::select! {
            res = kafka_server.accept() => {
                match res {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error accepting connection: {}", err);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
