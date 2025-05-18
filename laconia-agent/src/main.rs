use anyhow::Result;
use bytes::BytesMut;
use futures::StreamExt;
use laconia_agent::{
    RequestHeader, KafkaMessageCodec, RequestKind,
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

                let mut message = BytesMut::from(message);

                let request =
                    RequestKind::decode(&mut message).expect("should decode known messages");
                match request {
                    RequestKind::Metadata(_metadata) => println!("received metadata request"),
                    RequestKind::ApiVersions(_api_versions) => {
                        println!("received api versions request")
                    }
                }

                continue;

                let header = RequestHeader::decode(&mut message).unwrap();

                println!(
                    "Got key: {}, version: {}, client: {}",
                    header.api_key as i16, header.version, header.client_id
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
