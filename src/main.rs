use kafka_consumer_rust::KafkaConfig;

fn main() {
    let kafka_server: Vec<String> = vec!["localhost:9092".to_string()];
    let kafka_read_topic: String = String::from("test-topic");
    let kafka_write_topic: String = String::from("test-producer");

    let config = KafkaConfig::new(&kafka_server, &kafka_read_topic, &kafka_write_topic);
    println!("{}", config);
}
