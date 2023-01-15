use kafka::client::KafkaClient;
use kafka::consumer::Consumer;
//use crate::client::list_topics;

mod client;
mod consumer;

fn main() {
    let config = client::KafkaConfig::new();
    let client: KafkaClient = client::init_kafka_client(&config);
    //list_topics(&client);
    let mut kafka_consumer: Consumer = consumer::create_consumer(client, &config);

    //consumer::print_messages(&mut kafka_consumer, false);
    let messages = consumer::fetch_messages(&mut kafka_consumer, false).unwrap_or_else(|e| {
        panic!("Failed to fetch messages: {}", e);
    });

    for message in messages {
        println!("Message: {}", message);
    }
}
