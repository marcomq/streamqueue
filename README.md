# StreamQueue

`StreamQueue` is a Rust library designed to provide a unified and flexible interface for interacting with various message streams and queues.

## Purpose
The primary goal of `StreamQueue` is to offer a robust and easy-to-use library for developers who need to integrate with different message brokers like Kafka, RabbitMQ, NATS, MQTT, etc. within their Rust applications. While it shares conceptual similarities with systems like Watermill in the Go ecosystem, `StreamQueue` focuses less on event-driven architectures and more on being a fast and useful library to switch between message queues. It aims to abstract away the complexities of individual broker APIs, allowing you to send and receive messages consistently across different messaging technologies.

### Focus
- Easy to use
- Fast - it has some performance integration tests
- Easy to write tests for - has a memory endpoint to simplify unit tests

Whether you're building microservices, data pipelines, or distributed systems, `StreamQueue` provides the building blocks to handle message persistence, routing, and consumption with a common API.

## Features (Planned/Under Development)
*   **Unified API:** Interact with multiple message brokers through a single, consistent interface.
*   **Pluggable Backends:** Support for Kafka, RabbitMQ (AMQP), NATS (including JetStream), MQTT, and more.
*   **Message Transformation:** Capabilities for message serialization, deserialization, and content manipulation.
*   **Error Handling & Retries:** Robust mechanisms for dealing with transient failures.
*   **Integration with File Systems & HTTP:** Bridge messages to and from local files and HTTP endpoints.


# Status
Current status is work in progress. Don't use it without testing and fixing.
