#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use super::common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, run_test_with_docker, setup_logging, PERF_TEST_MESSAGE_COUNT,
};
use streamqueue::endpoints::mongodb::{MongoDbConsumer, MongoDbPublisher};
const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 10_000;
const PERF_TEST_CONCURRENCY: usize = 100;
const CONFIG_YAML: &str = r#"
sled_path: "/tmp/integration_test_db_mongodb"
dedup_ttl_seconds: 60

routes:
  memory_to_mongodb:
    in:
      memory: { topic: "test-in-mongodb" }
    out:
      mongodb: { url: "mongodb://localhost:27017", database: "streamqueue_test", collection: "test_collection" }

  mongodb_to_memory:
    in:
      mongodb: { url: "mongodb://localhost:27017", database: "streamqueue_test", collection: "test_collection" }
    out:
      memory: { topic: "test-out-mongodb", capacity: {out_capacity} }
"#;

pub async fn test_mongodb_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/mongodb.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_pipeline_test("mongodb", &config_yaml).await;
    })
    .await;
}

pub async fn test_mongodb_performance_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/mongodb.yml", || async {
        let config_yaml = CONFIG_YAML.replace(
            "{out_capacity}",
            &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
        );
        run_performance_pipeline_test("mongodb", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

pub async fn test_mongodb_performance_direct() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose/mongodb.yml", || async {
        let collection_name = "perf_mongodb_direct";
        let config = streamqueue::config::MongoDbConfig {
            url: "mongodb://localhost:27017".to_string(),
            database: "streamqueue_test_db".to_string(),
            ..Default::default()
        };

        // Ensure the collection is clean before the test
        let client = mongodb::Client::with_uri_str(&config.url).await.unwrap();
        client
            .database(&config.database)
            .collection::<mongodb::bson::Document>(collection_name)
            .drop()
            .await
            .ok();

        let publisher = Arc::new(
            MongoDbPublisher::new(&config, collection_name)
                .await
                .unwrap(),
        );
        measure_write_performance(
            "MONGODB",
            publisher,
            PERF_TEST_MESSAGE_COUNT_DIRECT,
            PERF_TEST_CONCURRENCY,
        )
        .await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        let consumer = Arc::new(tokio::sync::Mutex::new(
            MongoDbConsumer::new(&config, collection_name)
                .await
                .unwrap(),
        ));
        measure_read_performance(
            "MONGODB",
            consumer,
            PERF_TEST_MESSAGE_COUNT_DIRECT,
            PERF_TEST_CONCURRENCY,
        )
        .await;
    })
    .await;
}
