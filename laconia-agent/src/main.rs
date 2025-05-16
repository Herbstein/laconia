use anyhow::Result;
use bytes::BytesMut;
use futures::StreamExt;
use laconia_agent::{
    KafkaMessageCodec, KafkaHeader,
    protocol::{Decodable, Decoder, messages::ApiVersionsRequest},
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
                println!("Got a message of length: {}", message.len());

                let mut message = BytesMut::from(message);

                let header = KafkaHeader::decode(&mut message).unwrap();

                println!(
                    "Got key: {}, version: {}, client: {}",
                    header.key, header.version, header.client_id
                );

                let api_versions = ApiVersionsRequest::decode(&mut message, header.version)
                    .expect("couldn't decode metadata request");

                println!(
                    "Api version request - software name: '{}', software version: '{}'",
                    api_versions.client_software_name, api_versions.client_software_version
                )
            }
        });
    }

    Ok(())
}
