use crate::config::Config;
use std::path::Path;

pub fn load_config() -> Result<Config, config::ConfigError> {
    // Attempt to load .env file
    #[cfg(feature = "dotenv")]
    {
        dotenvy::dotenv().ok();
    }
    let config_file = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yml".to_string());

    let settings = config::Config::builder()
        // Start with default values
        .set_default("log_level", "info")?
        .set_default("sled_path", "/tmp/dedup_db")?
        .set_default("dedup_ttl_seconds", 86400)?
        // Load from a configuration file, if it exists.
        .add_source(config::File::from(Path::new(&config_file)).required(false))
        // Load from environment variables, which will override file and defaults.
        .add_source(
            config::Environment::default()
                .prefix("BRIDGE")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        )
        .build()?;
    settings.try_deserialize()
}


#[allow(unused_imports)]
mod tests {
    use super::*;
    use crate::config::{ConsumerEndpointType, KafkaConsumerEndpoint};
    use std::sync::Mutex;

    // A global mutex to ensure that tests modifying the environment run serially.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_config_from_env_vars() {
        // Lock the mutex to ensure this test has exclusive access to the environment variables.
        let _guard = ENV_MUTEX.lock().unwrap();
        // Clear the var first to avoid interference from other tests
        unsafe {
            std::env::remove_var("BRIDGE__LOG_LEVEL");
            std::env::set_var("BRIDGE__LOG_LEVEL", "trace");
            std::env::set_var("BRIDGE__LOGGER", "json");
            std::env::set_var("BRIDGE__SLED_PATH", "/tmp/env_test_db");
            std::env::set_var("BRIDGE__DEDUP_TTL_SECONDS", "300");

            // Route 0: Kafka to NATS
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__BROKERS",
                "env-kafka:9092",
            );
            // Source
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__GROUP_ID",
                "env-group",
            );
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__TOPIC",
                "env-in-topic",
            );
            // Sink
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUT__NATS__URL",
                "nats://env-nats:4222",
            );
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUT__NATS__SUBJECT",
                "env-out-subject",
            );
            // DLQ
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__BROKERS",
                "env-dlq-kafka:9092",
            );
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__GROUP_ID",
                "env-dlq-group",
            );
            std::env::set_var(
                "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__TOPIC",
                "env-dlq-topic",
            );

            // Load config
            let config = load_config().unwrap();

            // Assertions
            assert_eq!(config.log_level, "trace");
            assert_eq!(config.logger, "json");
            assert_eq!(config.sled_path, "/tmp/env_test_db");
            assert_eq!(config.dedup_ttl_seconds, 300);
            assert_eq!(config.routes.len(), 1);

            let (name, route) = config.routes.iter().next().unwrap();
            assert_eq!(name, "kafka_to_nats_from_env");

            // Assert source
            #[cfg(feature = "kafka")]
            {
                if let ConsumerEndpointType::Kafka(k) = &route.r#in.endpoint_type {
                    assert_eq!(k.config.brokers, "env-kafka:9092"); // group_id is now optional
                    assert_eq!(k.endpoint.topic.as_deref(), Some("env-in-topic"));
                } else {
                    panic!("Expected Kafka source endpoint");
                }
            }
        }
    }
}