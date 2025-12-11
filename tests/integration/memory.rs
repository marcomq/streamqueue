#![allow(dead_code)]

use super::common::{run_performance_pipeline_test, setup_logging};
const PERF_TEST_MESSAGE_COUNT: usize = 1250_000;
const PERF_TEST_CONCURRENCY: usize = 1;
const CONFIG_YAML: &str = r#"
sled_path: "/tmp/integration_test_db_mongodb"
dedup_ttl_seconds: 60

routes:
  memory_to_internal:
    deduplication_enabled: false
    in:
      memory: { topic: "test-in-internal" }
    out:
      memory: { topic: "test-inntermediate-memory", capacity: {out_capacity} }

  internal_to_memory:
    deduplication_enabled: false
    in:
      memory: { topic: "test-inntermediate-memory", capacity: {out_capacity}  }
    out:
      memory: { topic: "test-out-internal", capacity: {out_capacity} }
"#;

pub async fn test_memory_performance_pipeline() {
    setup_logging();
    let config_yaml = CONFIG_YAML.replace(
        "{out_capacity}",
        &(PERF_TEST_MESSAGE_COUNT + 1000).to_string(),
    );
    run_performance_pipeline_test("memory", &config_yaml, PERF_TEST_MESSAGE_COUNT).await;
}
