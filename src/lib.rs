use std::fmt::{Display, Formatter};
use kafka::client::KafkaClient;

///! # Kafka Client
/// This is a simple client for the Kafka protocol, to be used in the context of the Online Delivery Project.

/// General settings for the Kafka client.
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub read_topic: String,
    pub write_topic: String,
}

impl KafkaConfig {
    /// Constructor-like function to create a new KafkaConfig.
    ///
    /// # Arguments
    ///
    /// * `brokers` - A list of brokers to connect to as vector.
    /// * `read_topic` - The topic to read from.
    /// * `write_topic` - The topic to write to.
    ///
    /// # Example
    ///
    /// ```
    /// use kafka_consumer_rust::KafkaConfig;
    /// let config = KafkaConfig::new(
    ///    &vec!["localhost:9092".to_string()],
    ///   &"test-read".to_string(),
    ///  &"test-write".to_string(),
    /// );
    /// assert_eq!(config.brokers, vec!["localhost:9092".to_string()]);
    /// assert_eq!(config.read_topic, "test-read".to_string());
    /// assert_eq!(config.write_topic, "test-write".to_string());
    ///
    /// ```
    pub fn new(servers: &Vec<String>, read_topic: &String, write_topic: &String) -> KafkaConfig {
        KafkaConfig {
            brokers: servers.clone(),
            read_topic: read_topic.clone(),
            write_topic: write_topic.clone(),
        }
    }
}

impl Display for KafkaConfig {
    /// Display function for the KafkaConfig struct. Allows to print out the struct.
    /// # Example
    ///
    /// ```
    /// use kafka_consumer_rust::KafkaConfig;
    /// let config = KafkaConfig::new(
    ///   &vec!["localhost:9092".to_string()],
    ///  &"test-read".to_string(),
    /// &"test-write".to_string(),
    /// );
    ///
    /// println!("{}", config);
    /// ```
    ///
    /// will produce:
    ///
    /// ```text
    /// KafkaConfig with:
    ///     brokers: ["localhost:9092"],
    ///     read_topic: test-topic,
    ///     write_topic: test-producer
    /// ```
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KafkaConfig with:\n \
                brokers: {:?},\n \
                read_topic: {},\n \
                write_topic: {}\n",
            self.brokers, self.read_topic, self.write_topic
        )
    }
}

/// Create a new Kafka client and load the metadata.
///
/// # Arguments
///
///
pub fn init_kafka_client(config_params: &KafkaConfig) -> KafkaClient {
    let mut client = KafkaClient::new(config_params.brokers.clone());
    client.load_metadata_all().unwrap_or_else(|e| {
        panic!("Failed to load metadata: {}", e);
    });
    client
}



#[cfg(test)]
mod tests {
    use crate::KafkaConfig;

    #[test]
    fn test_kafka_config() {
        let config = KafkaConfig::new(
            &vec!["localhost:9092".to_string()],
            &"test-read".to_string(),
            &"test-write".to_string(),
        );
        assert_eq!(config.brokers, vec!["localhost:9092".to_string()]);
        assert_eq!(config.read_topic, "test-read".to_string());
        assert_eq!(config.write_topic, "test-write".to_string());
    }

    #[test]
    fn test_kafka_config_display() {
        let config = KafkaConfig::new(
            &vec!["localhost:9092".to_string()],
            &"test-read".to_string(),
            &"test-write".to_string(),
        );
        assert_eq!(format!("{}", config), "KafkaConfig with:\n \
                brokers: [\"localhost:9092\"],\n \
                read_topic: test-read,\n \
                write_topic: test-write\n");
    }
}