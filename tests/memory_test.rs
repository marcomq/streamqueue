use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use streamqueue::config::{
    Config, ConsumerEndpoint, ConsumerEndpointType, MemoryConfig, MemoryConsumerEndpoint,
    MemoryPublisherEndpoint, MetricsConfig, PublisherEndpoint, PublisherEndpointType, Route,
};

mod integration;

// run in release:
// cargo test --package streamqueue --test memory_test --features integration-test --release -- test_memory_to_memory_pipeline --exact --nocapture

#[tokio::test]
async fn test_memory_to_memory_pipeline() {
    integration::common::setup_logging();

    let num_messages = 500_000;
    let (messages_to_send, sent_message_ids) =
        integration::common::generate_test_messages(num_messages);

    let config = Config {
        metrics: MetricsConfig::disabled(),
        routes: HashMap::from([(
            "mem".to_string(),
            Route {
                input: ConsumerEndpoint {
                    endpoint_type: ConsumerEndpointType::Memory(MemoryConsumerEndpoint {
                        config: MemoryConfig {
                            topic: "mem-in".to_string(),
                            capacity: Some(200), // A reasonable capacity for the input channel
                        },
                    }),
                },
                output: PublisherEndpoint {
                    endpoint_type: PublisherEndpointType::Memory(MemoryPublisherEndpoint {
                        config: MemoryConfig {
                            topic: "mem-out".to_string(),
                            capacity: Some(num_messages + 10_000), // Ensure output can hold all messages
                        },
                    }),
                },
                dlq: None,
                concurrency: Some(1),
                deduplication_enabled: false,
            },
        )]),
        ..Default::default()
    };

    let mut bridge = streamqueue::Bridge::new(config);

    let (_name, route) = bridge.routes().next().unwrap(); // there is only one route here
    let in_channel = route.input.channel().unwrap();
    let out_channel = route.output.channel().unwrap();

    let bridge_handle = bridge.run();

    println!("--- Starting Memory-to-Memory Pipeline Test ---");
    let start_time = Instant::now();

    let fill_task = tokio::spawn(async move {
        in_channel.fill_messages(messages_to_send).await.unwrap();
        in_channel.close();
    });

    // Wait for the bridge to finish processing.
    let _ = tokio::time::timeout(Duration::from_secs(20), bridge_handle)
        .await
        .expect("Test timed out waiting for bridge to complete");

    // Ensure the fill task also completed without error
    fill_task.await.unwrap();

    let duration = start_time.elapsed();
    let received_ids: HashSet<_> = out_channel
        .drain_messages()
        .into_iter()
        .map(|m| m.message_id.to_string())
        .collect();

    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();
    println!(
        "Processed {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
    println!("-------------------------------------------------");

    assert_eq!(received_ids.len(), num_messages);
    assert_eq!(sent_message_ids, received_ids);
}
