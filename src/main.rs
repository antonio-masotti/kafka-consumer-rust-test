use kafka::consumer::Consumer;
use kafka::producer::Producer;

mod api;
mod client; // <-- This module contains the general configs for the client and some unit tests
mod consumer; // <-- This module contains the consumer logic and some unit tests
mod producer; // <-- This module contains the producer logic // <-- This module contains the API logic to test the POST Request

fn main() {
    let config = client::KafkaConfig::new();
    let mut kafka_consumer: Consumer = consumer::create_consumer(&config);
    let mut kafka_producer: Producer = producer::create_producer(&config);
    //consumer::print_messages(&mut kafka_consumer, false);
    let messages = consumer::fetch_messages(&mut kafka_consumer, false).unwrap_or_else(|e| {
        panic!("Failed to fetch messages: {}", e);
    });

    let mut counter = 0;
    for message in messages {
        //println!("Message: {}", message);

        let json_string = message.to_json();
        //println!("JSON: {}", json_string);

        api::send_request(json_string);

        // Resend to another topic (Just testing the producer part)
        let key = format!("test-key-{}", counter).to_string();
        producer::push_message(&message, key, &mut kafka_producer, &config).unwrap_or_else(|e| {
            panic!("Failed to push message: {}", e);
        });
        counter += 1;
    }
}
