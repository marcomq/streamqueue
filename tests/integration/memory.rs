#![allow(dead_code)]

use super::common::{
    run_performance_pipeline_test,
    setup_logging,
};
const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 10_000;
const PERF_TEST_MESSAGE_COUNT: usize = 50_000;
const PERF_TEST_CONCURRENCY: usize = 100;
const CONFIG_YAML: &str = r#"
sled_path: "/tmp/integration_test_db_mongodb"
dedup_ttl_seconds: 60

routes:
  memory_to_mongodb:
    in:
      memory: { topic: "test-in-mongodb" }
    out:
      memory: { topic: "test-inntermediate-mongodb", capacity: {out_capacity}  }

  mongodb_to_memory:
    in:
      memory: { topic: "test-inntermediate-mongodb", capacity: {out_capacity}  }
    out:
      memory: { topic: "test-out-mongodb", capacity: {out_capacity} }
"#;

pub async fn test_memory_performance_pipeline() {
    setup_logging();
    let config_yaml = CONFIG_YAML.replace(
        "{out_capacity}",
        &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
    );
    run_performance_pipeline_test("mongodb", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
}
