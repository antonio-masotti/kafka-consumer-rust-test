use crate::client::KafkaConfig;
use kafka::client::{FetchOffset, KafkaClient};
use kafka::consumer::{Consumer, MessageSets};
use std::str;
use serde::ser::StdError;
use kafka_consumer_rust::TestMessage;

pub fn create_consumer(client: KafkaClient, config: &KafkaConfig) -> Consumer {
    let consumer: Consumer = Consumer::from_client(client)
        .with_topic(config.read_topic.clone().to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .create()
        .unwrap_or_else(|e| {
            panic!("Failed to create consumer: {}", e);
        });
    consumer
}

/// Print the messages from the Kafka server and optionally consume them
///
/// # Arguments
///
/// * `kafka_consumer` - A Kafka Consumer struct.
/// * `consume_message` - A boolean indicating whether to consume the messages or not.
///
/// # Examples
///
/// In the simplest case, it just prints out the messages, each one looking like this
///
/// ~~~text
/// Message Topic: "test-topic"
/// Message: 5
/// Message Key: "test"
/// Message Value: "{\"_id\":\"63c184fcdbc7219b330870d7\",\"crmId\":\"355e0213-4ce5-4c74-8a8c-e96558f576a1\",\"isActive\":true,\"age\":55,\"name\":\"Hurley Meyer\",\"gender\":\"male\",\"company\":\"ONTAGENE\",\"email\":\"hurleymeyer@ontagene.com\"}"
/// ~~~
pub fn print_messages(kafka_consumer: &mut Consumer, consume_message: bool) {

    'messageLoop: loop {
        let message_sets: MessageSets = kafka_consumer.poll().unwrap_or_else(|e| {
            panic!("Failed to poll: {}", e);
        });

        if message_sets.is_empty() {
            println!("Message sets exausted... no more messages to retrieve!");
            break 'messageLoop;
        }

        for message_group in message_sets.iter() {
            println!("Message Topic: {:?}", message_group.topic());
            println!("Message: {:?}", message_group.messages().len());

            for message in message_group.messages() {
                    let m_key = str::from_utf8(message.key).unwrap();
                    let m_value = str::from_utf8(message.value).unwrap();
                    println!("Message Key: {:?}", m_key);
                    println!("Message Value: {:?}", m_value);
             }
            if consume_message {
                kafka_consumer.consume_messageset(message_group).unwrap_or_else(|e| {
                    panic!("Failed to consume message: {}", e);
                });
            }
            }
        }
    }


/// Fetch the messages from the Kafka server and optionally consume them.
/// Once fetched, the messages are serialized into a vector of [TestMessage](kafka-consumer-rust::client::TestMessage) structs.
pub fn fetch_messages(kafka_consumer: &mut Consumer, consume_message: bool) -> Result<Vec<TestMessage>, Box<dyn StdError>>
{
    let mut results: Vec<TestMessage> = Vec::new();
    'messageLoop: loop {
        let message_sets: MessageSets = kafka_consumer.poll()?;

        if message_sets.is_empty() {
            println!("Message sets exausted... no more messages to retrieve!");
            break 'messageLoop;
        }

        for message_group in message_sets.iter() {

            for message in message_group.messages() {
                let m_key = str::from_utf8(message.key).unwrap();
                let m_value = str::from_utf8(message.value).unwrap();

                let test_message: TestMessage = serde_json::from_str(m_value)?;
                results.push(test_message);
            }
            if consume_message {
                kafka_consumer.consume_messageset(message_group).unwrap_or_else(|e| {
                    panic!("Failed to consume message: {}", e);
                });
            }
        }
    }
    Ok(results)
}

