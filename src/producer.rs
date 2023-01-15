use crate::client;
use crate::client::KafkaConfig;
use kafka::client::RequiredAcks;
use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record};
use kafka_consumer_rust::TestMessage;
use std::time::Duration;

pub fn create_producer(config: &KafkaConfig) -> Producer {
    let client = client::init_kafka_client(&config);
    let mut producer = Producer::from_client(client)
        .with_ack_timeout(Duration::from_secs(3))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap_or_else(|e| {
            panic!("Failed to create producer: {}", e);
        });
    producer
}

pub fn push_message(
    data: &TestMessage,
    key: String,
    producer: &mut Producer,
    config: &KafkaConfig,
) -> Result<(), KafkaError> {
    let message = serde_json::to_string(data).unwrap_or_else(|e| {
        panic!("Failed to serialize message: {}", e);
    });
    let topic = &config.write_topic;
    let record = Record::from_key_value(topic, key, message);
    producer.send(&record)?;

    println!("Message sent to topic: {}", topic);

    Ok(())
}
