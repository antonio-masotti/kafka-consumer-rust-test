use kafka::client::{FetchOffset, KafkaClient};
use kafka::consumer::{Consumer, MessageSetsIter};
use std::str;
use kafka_consumer_rust::TestMessage;
use crate::consumer::create_consumer;


mod client;
mod consumer;

fn main() {
    let (kafka_server, kafka_read_topic, kafka_write_topic) = client::setup_variables();

    let config = client::KafkaConfig::new(&kafka_server, &kafka_read_topic, &kafka_write_topic);
    let mut client: KafkaClient = client::init_kafka_client(&config);

    // let mut cs: Consumer = Consumer::from_client(client)
    //     .with_topic(config.read_topic.clone().to_owned())
    //     .with_fallback_offset(FetchOffset::Earliest)
    //     .create()
    //     .unwrap_or_else(|e| {
    //         panic!("Failed to create consumer: {}", e);
    //     });
    let mut cs: Consumer = create_consumer(client, &config);
    

    let res = cs.poll().unwrap_or_else(|e| {
        panic!("Failed to poll: {}", e);
    });
    let mut m_iter: MessageSetsIter = res.iter();
    let message_set = m_iter.next();
    match message_set {
        Some(m) => {
            println!("Message Topic: {:?}", m.topic());
            println!("Message: {:?}", m.messages().len());
            for message in m.messages() {
                let m_key = str::from_utf8(message.key).unwrap();
                let m_value = str::from_utf8(message.value).unwrap();
                println!("Message Key: {:?}", m_key);
                println!("Message Value: {:?}", m_value);

                let v: TestMessage = serde_json::from_str(&m_value).unwrap();
                println!("Message Value: {}", v.crmId);
                println!("Message Value: {}", v.name);
                println!("Message Value: {}", v.age);
            }
        }
        None => println!("No message"),
    }
}
