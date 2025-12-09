//  streamqueue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/streamqueue

#[cfg(feature = "amqp")]
pub mod amqp;
pub mod file;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;
pub mod static_endpoint;

use crate::config::{
    ConsumerEndpoint, ConsumerEndpointType, PublisherEndpoint, PublisherEndpointType, Route,
};
use crate::consumers::MessageConsumer;
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Result};
use std::sync::Arc;

/// Creates a `MessageConsumer` based on the route's "in" configuration.
pub async fn create_consumer_from_route(
    route_name: &str,
    endpoint: &ConsumerEndpoint,
) -> Result<Box<dyn MessageConsumer>> {
    match &endpoint.endpoint_type {
        #[cfg(feature = "kafka")]
        ConsumerEndpointType::Kafka(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(kafka::KafkaConsumer::new(&cfg.config, topic)?))
        }
        #[cfg(feature = "nats")]
        ConsumerEndpointType::Nats(cfg) => {
            let subject = cfg.endpoint.subject.as_deref().unwrap_or(route_name);
            let stream_name = cfg
                .endpoint
                .stream
                .as_deref()
                .or(cfg.config.default_stream.as_deref())
                .ok_or_else(|| {
                    anyhow!(
                        "[route:{}] NATS consumer must specify a 'stream' or have a 'default_stream'",
                        route_name
                    )
                })?;
            Ok(Box::new(
                nats::NatsConsumer::new(&cfg.config, stream_name, subject).await?,
            ))
        }
        #[cfg(feature = "amqp")]
        ConsumerEndpointType::Amqp(cfg) => {
            let queue = cfg.endpoint.queue.as_deref().unwrap_or(route_name);
            Ok(Box::new(amqp::AmqpConsumer::new(&cfg.config, queue).await?))
        }
        #[cfg(feature = "mqtt")]
        ConsumerEndpointType::Mqtt(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(
                mqtt::MqttConsumer::new(&cfg.config, topic, route_name).await?,
            ))
        }
        ConsumerEndpointType::File(cfg) => {
            Ok(Box::new(file::FileConsumer::new(&cfg.config).await?))
        }
        #[cfg(feature = "http")]
        ConsumerEndpointType::Http(cfg) => {
            Ok(Box::new(http::HttpConsumer::new(&cfg.config).await?))
        }
        ConsumerEndpointType::Static(cfg) => {
            Ok(Box::new(static_endpoint::StaticRequestConsumer::new(cfg)?))
        }
        ConsumerEndpointType::Memory(cfg) => Ok(Box::new(memory::MemoryConsumer::new(
            &memory::get_or_create_channel(&cfg.config),
        ))),
        #[cfg(feature = "mongodb")]
        ConsumerEndpointType::MongoDb(cfg) => {
            let collection = cfg.endpoint.collection.as_deref().unwrap_or(route_name);
            Ok(Box::new(
                mongodb::MongoDbConsumer::new(&cfg.config, collection).await?,
            ))
        }
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported consumer endpoint type",
            route_name
        )),
    }
}

/// Creates a `MessagePublisher` based on the route's "out" configuration.
pub async fn create_publisher_from_route(
    route_name: &str,
    endpoint: &PublisherEndpoint,
) -> Result<Arc<dyn MessagePublisher>> {
    match &endpoint.endpoint_type {
        #[cfg(feature = "kafka")]
        PublisherEndpointType::Kafka(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                kafka::KafkaPublisher::new(&cfg.config, topic).await?,
            ))
        }
        #[cfg(feature = "nats")]
        PublisherEndpointType::Nats(cfg) => {
            let subject = cfg.endpoint.subject.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                nats::NatsPublisher::new(&cfg.config, subject, cfg.endpoint.stream.as_deref())
                    .await?,
            ))
        }
        #[cfg(feature = "amqp")]
        PublisherEndpointType::Amqp(cfg) => {
            let queue = cfg.endpoint.queue.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                amqp::AmqpPublisher::new(&cfg.config, queue).await?,
            ))
        }
        #[cfg(feature = "mqtt")]
        PublisherEndpointType::Mqtt(cfg) => {
            let topic = cfg.endpoint.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                mqtt::MqttPublisher::new(&cfg.config, topic, route_name).await?,
            ))
        }
        PublisherEndpointType::File(cfg) => {
            Ok(Arc::new(file::FilePublisher::new(&cfg.config).await?))
        }
        #[cfg(feature = "http")]
        PublisherEndpointType::Http(cfg) => {
            let mut sink = http::HttpPublisher::new(&cfg.config).await?;
            if let Some(url) = &cfg.endpoint.url {
                sink = sink.with_url(url);
            }
            Ok(Arc::new(sink))
        }
        PublisherEndpointType::Static(cfg) => Ok(Arc::new(
            static_endpoint::StaticEndpointPublisher::new(cfg)?,
        )),
        PublisherEndpointType::Memory(cfg) => Ok(Arc::new(memory::MemoryPublisher::new(
            &memory::get_or_create_channel(&cfg.config),
        ))),
        #[cfg(feature = "mongodb")]
        PublisherEndpointType::MongoDb(cfg) => {
            let collection = cfg.endpoint.collection.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                mongodb::MongoDbPublisher::new(&cfg.config, collection).await?,
            ))
        }
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported publisher endpoint type",
            route_name
        )),
    }
}

/// Creates a `MessagePublisher` for the DLQ if configured.
#[allow(unused_variables)]
pub async fn create_dlq_from_route(
    route: &Route,
    name: &str,
) -> Result<Option<Arc<dyn MessagePublisher>>> {
    #[cfg(feature = "kafka")]
    {
        if let Some(dlq_config) = &route.dlq {
            tracing::info!("DLQ configured for route {}", name);
            let topic = dlq_config.kafka.endpoint.topic.as_deref().unwrap_or("dlq");
            let publisher = kafka::KafkaPublisher::new(&dlq_config.kafka.config, topic).await?;
            return Ok(Some(Arc::new(publisher)));
        }
    }
    Ok(None)
}
