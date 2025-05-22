use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use laconia_agent::{
    ConnectionState, KafkaMessageCodec, KafkaRequest, KafkaResponse,
    protocol::{
        handlers::{api_versions::ApiVersionsHandler, metadata::MetadataRequestHandler},
        registry::MessageRegistry,
    },
};
use tokio::net::TcpListener;
use tokio_util::codec::Decoder as _;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    let mut registry = MessageRegistry::new();
    registry.register(3, MetadataRequestHandler);
    registry.register(18, ApiVersionsHandler);

    let registry = Arc::new(registry);

    while let Ok((stream, _)) = listener.accept().await {
        let registry = registry.clone();

        tokio::spawn(async move {
            let mut connection_state = ConnectionState::new(registry.clone());

            let mut stream = KafkaMessageCodec.framed(stream);

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
    }

    Ok(())
}
