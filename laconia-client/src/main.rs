use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
};

fn main() -> anyhow::Result<()> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:8080")
        .create::<BaseConsumer>()?;

    let metadata = consumer.fetch_metadata(None, None)?;
    println!("Brokers:");
    for broker in metadata.brokers() {
        println!("\t{}: {}", broker.id(), broker.host());
    }
    println!("Topics:");
    for topic in metadata.topics() {
        println!("\tPartitions:");
        for partition in topic.partitions() {
            println!(
                "\t\t{} - {}: {}",
                topic.name(),
                partition.id(),
                partition.leader()
            );
        }
    }

    Ok(())
}
