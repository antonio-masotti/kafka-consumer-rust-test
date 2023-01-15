use crate::client::KafkaConfig;
use kafka::client::{FetchOffset, KafkaClient};
use kafka::consumer::{Consumer, MessageSet, MessageSets, MessageSetsIter};
use std::str;

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

pub fn print_messages(kafka_consumer: &mut Consumer) {
    let message_set: MessageSets = kafka_consumer.poll().unwrap_or_else(|e| {
        panic!("Failed to poll: {}", e);
    });

    let mut message_iterator: MessageSetsIter = message_set.iter();
    let message_set: Option<MessageSet> = message_iterator.next();
    match message_set {
        Some(m) => {
            println!("Message Topic: {:?}", m.topic());
            println!("Message: {:?}", m.messages().len());
            for message in m.messages() {
                println!(
                    "Found {} messages in topic {}",
                    m.messages().len(),
                    m.topic()
                );
                let m_key = str::from_utf8(message.key).unwrap();
                let m_value = str::from_utf8(message.value).unwrap();
                println!("Message Key: {:?}", m_key);
                println!("Message Value: {:?}", m_value);
            }
        }
        None => println!("No message"),
    }
}
