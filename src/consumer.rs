use kafka::client::{FetchOffset, KafkaClient};
use kafka::consumer::Consumer;
use crate::client::KafkaConfig;

pub fn create_consumer(client: KafkaClient, config: &KafkaConfig) -> Consumer {
    let mut consumer: Consumer = Consumer::from_client(client)
        .with_topic(config.read_topic.clone().to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .create()
        .unwrap_or_else(|e| {
            panic!("Failed to create consumer: {}", e);
        });
    consumer
}


