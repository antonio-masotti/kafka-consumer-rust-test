use kafka_consumer_rust::{init_kafka_client, KafkaConfig, setup_variables};
use kafka::client::KafkaClient;

fn main() {
    let (kafka_server, kafka_read_topic, kafka_write_topic) = setup_variables();

    let config = KafkaConfig::new(&kafka_server, &kafka_read_topic, &kafka_write_topic);
    let _client: KafkaClient = init_kafka_client(&config);

}

