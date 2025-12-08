
pub mod common;
#[cfg(feature = "amqp")]
pub mod amqp;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "nats")]
pub mod nats;
#[cfg(all(feature = "nats", feature = "kafka", feature = "amqp", feature = "mqtt", feature = "http"))]
pub mod all_endpoints;