#![allow(dead_code)] // This module contains helpers used by various integration tests.
use async_channel::{bounded, Receiver, Sender};
use chrono;
use serde_json::json;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use streamqueue::consumers::{CommitFunc, MessageConsumer};
use streamqueue::model::CanonicalMessage;
use streamqueue::publishers::MessagePublisher;
use tempfile::tempdir;

use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use metrics_util::MetricKind;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use streamqueue::config::Config as AppConfig;
use streamqueue::endpoints::memory::MemoryChannel;
use streamqueue::Bridge;

/// A helper for capturing and querying metrics during tests.
pub struct TestMetrics {
    snapshotter: Snapshotter,
    cumulative_counters: Mutex<HashMap<(String, String), u64>>,
}

impl TestMetrics {
    pub fn new() -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::set_global_recorder(recorder).expect("Failed to install testing recorder");
        Self {
            snapshotter,
            cumulative_counters: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_cumulative_counter(&self, name: &str, route: &str) -> u64 {
        let snapshot = self.snapshotter.snapshot();
        let mut cumulative_counters = self.cumulative_counters.lock().unwrap();
        for (key, _, _, value) in snapshot.into_vec() {
            if key.kind() == MetricKind::Counter && key.key().name() == name {
                if key
                    .key()
                    .labels()
                    .any(|l| l.key() == "route" && l.value() == route)
                {
                    let entry = cumulative_counters
                        .entry((name.to_string(), route.to_string()))
                        .or_insert(0);
                    if let DebugValue::Counter(delta_value) = value {
                        *entry += delta_value;
                    }
                }
            }
        }
        *cumulative_counters
            .get(&(name.to_string(), route.to_string()))
            .unwrap_or(&0)
    }
}

pub struct DockerCompose {
    compose_file: String,
}

impl DockerCompose {
    pub fn new(compose_file: &str) -> Self {
        Self {
            compose_file: compose_file.to_string(),
        }
    }

    pub fn up(&self) {
        println!(
            "Starting docker-compose services from {}...",
            self.compose_file
        );
        let status = Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("up")
            .arg("-d")
            .arg("--wait")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to start docker-compose");

        assert!(status.success(), "docker-compose up --wait failed");
        println!("Services from {} should be up.", self.compose_file);
    }

    pub fn down(&self) {
        println!(
            "Stopping docker-compose services from {}...",
            self.compose_file
        );
        Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("down")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to stop docker-compose");
        println!("Services from {} stopped.", self.compose_file);
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        self.down();
    }
}

pub fn read_and_drain_memory_channel(
    channel: &streamqueue::endpoints::memory::MemoryChannel,
) -> HashSet<String> {
    channel
        .drain_messages()
        .into_iter()
        .map(|msg| msg.message_id.to_string())
        .collect()
}

pub fn generate_test_messages(num_messages: usize) -> (Vec<CanonicalMessage>, HashSet<String>) {
    let mut sent_messages = HashSet::new();
    let mut messages = Vec::new();

    for i in 0..num_messages {
        let payload = json!({ "message_num": i, "test_id": "integration" });
        let msg = CanonicalMessage::from_json(payload.clone()).unwrap();
        sent_messages.insert(msg.message_id.to_string());
        messages.push(msg);
    }
    (messages, sent_messages)
}

/// A test harness to simplify integration testing of bridge pipelines.
struct TestHarness {
    _temp_dir: tempfile::TempDir,
    bridge: Bridge,
    shutdown_tx: tokio::sync::watch::Sender<()>,
    out_route_name: String,
    in_channel: MemoryChannel,
    out_channel: MemoryChannel,
    sent_message_ids: HashSet<String>,
    messages_to_send: Vec<CanonicalMessage>,
}

impl TestHarness {
    /// Creates a new TestHarness for a given broker and configuration.
    fn new(mut test_config: AppConfig, num_messages: usize) -> Self {
        let temp_dir = tempdir().unwrap();
        let (messages_to_send, sent_message_ids) = generate_test_messages(num_messages);

        // Ensure sled path is unique for each test run to avoid conflicts.
        test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();

        let bridge = Bridge::new(test_config);
        let shutdown_tx = bridge.get_shutdown_handle();

        // Find the route that goes from memory to the broker.
        let (_in_route_name, route_to_broker) = &bridge
            .routes()
            .find(|(name, _)| name.starts_with("memory_to_"))
            .map(|(name, route)| (name.clone(), route.clone()))
            .unwrap();

        // Find the route that goes from the broker back to memory.
        let (out_route_name, route_from_broker) = bridge
            .routes()
            .find(|(name, _)| name.ends_with("_to_memory"))
            .map(|(name, route)| (name.clone(), route.clone()))
            .unwrap();

        // The input to the system is the input of the `memory_to_*` route.
        let in_channel = route_to_broker.input.channel().unwrap();

        // The final output from the system is the output of the `*_to_memory` route.
        let out_channel = route_from_broker.output.channel().unwrap();

        Self {
            _temp_dir: temp_dir,
            bridge,
            shutdown_tx,
            out_route_name: out_route_name.to_string(),
            in_channel,
            out_channel,
            sent_message_ids,
            messages_to_send,
        }
    }

    /// Sends all generated test messages to the input channel.
    async fn send_messages(&self) {
        self.in_channel
            .fill_messages(self.messages_to_send.clone())
            .await
            .unwrap();
    }
}

pub async fn run_pipeline_test(broker_name: &str, config_yaml: &str) {
    run_pipeline_test_internal(broker_name, config_yaml, 5, false).await;
}

pub async fn run_performance_pipeline_test(
    broker_name: &str,
    config_yaml: &str,
    num_messages: usize,
) {
    run_pipeline_test_internal(broker_name, config_yaml, num_messages, true).await;
}

async fn run_pipeline_test_internal(
    broker_name: &str,
    config_yaml: &str,
    num_messages: usize,
    is_performance_test: bool,
) {
    let app_config: AppConfig =
        serde_yaml_ng::from_str(config_yaml).expect("Failed to parse YAML config");
    let mut harness = TestHarness::new(app_config, num_messages);
    let metrics = TestMetrics::new();

    let bridge_handle = harness.bridge.run();
    let start_time = std::time::Instant::now();

    harness.send_messages().await;

    // Wait for all messages to be processed by checking the metrics.
    let timeout = if is_performance_test {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(30)
    };
    let wait_start = Instant::now();
    while wait_start.elapsed() < timeout {
        let received_count = metrics
            .get_cumulative_counter("bridge_messages_received_total", &harness.out_route_name);
        if received_count >= num_messages as u64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if harness.shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }

    let received_ids = read_and_drain_memory_channel(&harness.out_channel);
    let duration = start_time.elapsed();

    if is_performance_test {
        let messages_per_second = num_messages as f64 / duration.as_secs_f64();
        println!("\n--- {} Performance Test Results ---", broker_name);
        println!(
            "Processed {} messages in {:.3} seconds.",
            received_ids.len(),
            duration.as_secs_f64()
        );
        println!("Rate: {:.2} messages/second", messages_per_second);
        println!("--------------------------------\n");
    }

    assert_eq!(
        received_ids.len(),
        num_messages,
        "TEST FAILED for [{}]: Expected {} messages, but found {}.",
        broker_name,
        num_messages,
        received_ids.len()
    );
    assert_eq!(
        harness.sent_message_ids, received_ids,
        "TEST FAILED for [{}]: The set of received message IDs does not match the set of sent message IDs.",
        broker_name
    );

    println!("Successfully verified {} route!", broker_name);

    let _ = bridge_handle.await;
}

static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

pub fn setup_logging() {
    // Using a std::sync::Once ensures this is only run once per test binary.
    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        let file_appender = tracing_appender::rolling::never("logs", "integration_test.log");
        let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

        *LOG_GUARD.lock().unwrap() = Some(guard);

        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking_writer)
            .with_ansi(false);

        let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .with(stdout_layer)
            .init();
    });
}

/// A test harness that manages the lifecycle of Docker containers for a single test.
/// It ensures that `docker-compose up` is run before the test and `docker-compose down`
/// is run after, even if the test panics.
pub async fn run_test_with_docker<F, Fut>(compose_file: &str, test_fn: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let _docker = DockerCompose::new(compose_file);
    // Give some time for docker to be ready
    _docker.up();
    test_fn().await;
}

pub const PERF_TEST_MESSAGE_COUNT: usize = 20_000;
pub const PERF_TEST_CONCURRENCY: usize = 100;

pub fn generate_message() -> CanonicalMessage {
    CanonicalMessage::from_json(json!({ "perf_test": true, "ts": chrono::Utc::now().to_rfc3339() }))
        .unwrap()
}

pub async fn measure_write_performance(
    name: &str,
    publisher: Arc<dyn MessagePublisher>,
    num_messages: usize,
    concurrency: usize,
) {
    println!("\n--- Measuring Write Performance for {} ---", name);
    let (tx, rx): (Sender<CanonicalMessage>, Receiver<CanonicalMessage>) = bounded(concurrency * 2);

    tokio::spawn(async move {
        for _ in 0..num_messages {
            if tx.send(generate_message()).await.is_err() {
                break;
            }
        }
        tx.close();
    });

    let start_time = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let rx_clone = rx.clone();
        let publisher_clone = publisher.clone();
        tasks.spawn(async move {
            while let Ok(message) = rx_clone.recv().await {
                // Loop to retry sending the message in case of backpressure.
                // This is common with async clients like rumqttc where the internal
                // buffer can fill up under high load.
                let mut first_try = true;
                loop {
                    let res = publisher_clone.send(message.clone()).await;
                    if let Err(e) = res {
                        if first_try {
                            eprintln!("Error sending message: {}", e);
                        }
                        first_try = false;
                    } else {
                        break;
                    }
                    // Backpressure detected, yield and retry.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
    publisher.flush().await.unwrap();

    let duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Wrote {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}

/// A mock consumer that does nothing, useful for testing publishers in isolation.
#[derive(Clone)]
pub struct MockConsumer;

#[async_trait::async_trait]
impl MessageConsumer for MockConsumer {
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        // This consumer will block forever, which is fine for tests that only need a publisher.
        // It prevents the route from exiting immediately.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        unreachable!();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub async fn measure_read_performance(
    name: &str,
    consumer: Arc<tokio::sync::Mutex<dyn MessageConsumer>>,
    num_messages: usize,
    concurrency: usize,
) {
    println!("\n--- Measuring Read Performance for {} ---", name);
    let messages_to_receive = Arc::new(tokio::sync::Semaphore::new(num_messages));
    let start_time = Instant::now();

    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let consumer_clone = consumer.clone();
        let semaphore_clone = messages_to_receive.clone();

        tasks.spawn(async move {
            loop {
                // Use `try_acquire` to avoid blocking forever.
                // If there are no more permits, the worker's job is done.
                match semaphore_clone.try_acquire() {
                    Ok(permit) => {
                        permit.forget();
                    }
                    Err(_) => {
                        break; // No more permits, exit the loop.
                    }
                }
                if semaphore_clone.try_acquire().is_err() {
                    break; // No more permits, exit the loop.
                }

                match consumer_clone.lock().await.receive().await {
                    Ok((_, commit)) => {
                        // Spawn the commit to a separate task to allow the worker
                        // to immediately start receiving the next message.
                        tokio::spawn(async move {
                            commit(None).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("Error receiving message: {}. Worker stopping.", e);
                        break; // Exit on consumer error.
                    }
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    let duration: Duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Read {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}
