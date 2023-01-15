use kafka::client::KafkaClient;
use std::fmt::{Display, Formatter};

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

/// Setup main variables for [KafkaConfig]
///
/// This function will populate and return the variables for
/// the KafkaConfig struct.
/// It will try to read them from the environment variables and if not found, will fallback to the default values.
///
/// # Default values
///
/// * `KAFKA_SERVER` - localhost:9092
/// * `KAFKA_READ_TOPIC` - test-topic
/// * `KAFKA_WRITE_TOPIC` - test-producer
///
pub fn setup_variables() -> (Vec<String>, String, String) {
    let kafka_server = vec![std::env::var("KAFKA_SERVER").unwrap_or("localhost:9092".to_string())];
    let kafka_read_topic = std::env::var("KAFKA_READ_TOPIC").unwrap_or("test-topic".to_string());
    let kafka_write_topic = std::env::var("KAFKA_WRITE_TOPIC").unwrap_or("test-producer".to_string());
    (kafka_server, kafka_read_topic, kafka_write_topic)
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

// ----------------------------------------------------------------
// -------------------------- UNIT TESTS --------------------------
// ----------------------------------------------------------------

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
        assert_eq!(
            format!("{}", config),
            "KafkaConfig with:\n \
                brokers: [\"localhost:9092\"],\n \
                read_topic: test-read,\n \
                write_topic: test-write\n"
        );
    }

    #[test]
    fn test_setup_variables_with_defaults() {
        let (kafka_server, kafka_read_topic, kafka_write_topic) = super::setup_variables();
        assert_eq!(kafka_server, vec!["localhost:9092".to_string()], "Kafka server should be localhost:9092");
        assert_eq!(kafka_read_topic, "test-topic".to_string(), "Kafka read topic should be test-topic");
        assert_eq!(kafka_write_topic, "test-producer".to_string(), "Kafka write topic should be test-producer");
    }

    #[test]
    fn test_setup_variables_with_env() {
        std::env::set_var("KAFKA_SERVER", "localhost:9082");
        std::env::set_var("KAFKA_READ_TOPIC", "test-from-env");
        std::env::set_var("KAFKA_WRITE_TOPIC", "test-producer-from-env");
        let (kafka_server, kafka_read_topic, kafka_write_topic) = super::setup_variables();
        assert_eq!(kafka_server, vec!["localhost:9082".to_string()], "KAFKA_SERVER should be localhost:9082, but is {}", kafka_server[0]);
        assert_eq!(kafka_read_topic, "test-from-env".to_string(), "KAFKA_READ_TOPIC should be test-from-env, but is {}", kafka_read_topic);
        assert_eq!(kafka_write_topic, "test-producer-from-env".to_string(), "KAFKA_WRITE_TOPIC should be test-producer-from-env, but is {}", kafka_write_topic);

        std::env::remove_var("KAFKA_SERVER");
        std::env::remove_var("KAFKA_READ_TOPIC");
        std::env::remove_var("KAFKA_WRITE_TOPIC");
    }
}
