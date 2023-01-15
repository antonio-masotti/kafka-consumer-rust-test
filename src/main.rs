use kafka::client::{FetchOffset, KafkaClient};
use kafka::consumer::{Consumer, Message, MessageSet, MessageSetsIter};
use kafka_consumer_rust::{init_kafka_client, setup_variables, KafkaConfig};
use serde::{Deserialize, Serialize};
use std::str;

#[derive(Serialize, Deserialize)]
struct TestMessage {
    _id: String,
    crmId: String,
    isActive: bool,
    age: i32,
    name: String,
    company: String,
    gender: String,
    email: String,
}

// impl Display for TestMessage {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!("TestMessage with:\n\
//         ID: {}\
//         and Name: {}\
//         ", self._id, self.name)
//     }
// }

fn main() {
    let (kafka_server, kafka_read_topic, kafka_write_topic) = setup_variables();

    let config = KafkaConfig::new(&kafka_server, &kafka_read_topic, &kafka_write_topic);
    let mut client: KafkaClient = init_kafka_client(&config);
    client.load_metadata_all().unwrap_or_else(|e| {
        panic!("Failed to load metadata: {}", e);
    });

    let mut cs: Consumer = Consumer::from_client(client)
        .with_topic(config.read_topic.clone().to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .create()
        .unwrap_or_else(|e| {
            panic!("Failed to create consumer: {}", e);
        });

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
