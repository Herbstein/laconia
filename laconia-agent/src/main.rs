use anyhow::Result;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use laconia_agent::{
    ApiKey, KafkaMessageCodec, KafkaRequest, KafkaResponse, RequestKind, ResponseHeader,
    ResponseKind,
    protocol::{
        Decoder,
        messages::{ApiVersionsApiKeys, ApiVersionsResponse},
    },
};
use tokio::net::TcpListener;
use tokio_util::codec::Decoder as _;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
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
                    KafkaRequest::decode(&mut message).expect("should decode known messages");
                println!("{:?}", request);

                if let RequestKind::ApiVersions(_request) = request.request {
                    let api_keys = ApiKey::all()
                        .map(|api_key| {
                            let versions = api_key.versions();
                            ApiVersionsApiKeys {
                                api_key,
                                min_version: versions.min,
                                max_version: versions.max,
                                tagged_fields: Default::default(),
                            }
                        })
                        .collect();
                    let response = ApiVersionsResponse {
                        error_code: 0,
                        api_keys,
                        throttle_time_ms: 0,
                        tagged_fields: Default::default(),
                    };

                    let response = KafkaResponse {
                        header: ResponseHeader {
                            correlation_id: request.header.correlation_id,
                        },
                        response: ResponseKind::ApiVersions(response),
                    };

                    stream.send(response).await.unwrap();
                }
            }
        });
    }

    Ok(())
}
