# **LumenLog: Distributed Polyglot Observability Pipeline**

## **Category**
Distributed Systems / Security Engineering

A high-performance observability pipeline demonstrating cross-language data serialization and high-throughput persistence. This system serves as the central nervous system for unified logging across Go and Rust environments.

## **Architecture**

* **Producer (Rust):** Serializes system heartbeats using Protobuf and streams to Redpanda[cite: 10]
* **Producer (Go):** Sentinel WAF bridges security events via JSON-to-Protobuf translation[cite: 10]
* **Broker (Redpanda):** Kafka-compatible message broker for high-speed event streaming[cite: 10]
* **Ingestor (Go):** Consumes binary data, decodes via Protobuf, and batches writes to ClickHouse[cite: 3, 10]
* **Alerter (Go):** Real-time sidecar service that triggers Discord notifications for security events[cite: 10]
* **Storage (ClickHouse):** Columnar database optimized for real-time analytical queries[cite: 10]

## **Data Flow**

1. **Rust Agent** generates system logs -> Serialized with Protobuf -> Redpanda[cite: 10]
2. **Sentinel Proxy** generates security events -> Bridged to Ingestor -> Redpanda[cite: 8, 10]
3. **Go Ingestor** consumes messages -> Decodes and batches -> ClickHouse[cite: 3, 10]
4. **Go Alerter** monitors stream -> Filters for "SECURITY" level -> Discord API[cite: 10]

## **Core Features**

* **End-to-End Pipeline:** Fully integrated cross-language data flow[cite: 10]
* **Unified Schema:** Shared Protobuf definitions for Rust and Go services[cite: 10]
* **Smart Alerting:** Intelligent filtering that suppresses "Allowed" traffic noise while triggering Discord alerts only for verified security threats.[cite: 3]
* **Identity Awareness:** Integrated with the Identity Provider (IdP) to capture and store `user_id` or `anonymous` tags in every log entry for full auditability.[cite: 3]
* **High-Throughput Persistence:** Batched inserts into ClickHouse for analytical scale[cite: 3, 10]
* **Containerized Infrastructure:** Entire stack managed via Docker Compose[cite: 10, 7]

## **Latest Update: The Security & Identity Bridge**

This update integrates **Sentinel Proxy** and **Identity Verification** into the pipeline, transforming LumenLog into a Security Operations Platform.

* **Filtered Notifications:** Refactored the Alerter to ignore routine traffic, preventing Discord spam while maintaining 100% visibility on attacks.[cite: 3]
* **JWT Integration:** Ingestor now processes user identity passed through the Sentinel Zero Trust middleware.[cite: 3, 8]
* **Discord Webhook integration** for instant threat visibility[cite: 10]
* Unified system health (Rust) and security data (Go) into a single ClickHouse schema[cite: 10]

## **Database Schema (lumen_db.logs)**

* **service_name:** String (e.g., 'rust-agent', 'sentinel-proxy')[cite: 10]
* **host:** String[cite: 10]
* **level:** String (INFO, WARN, SECURITY)[cite: 10]
* **message:** String[cite: 10]
* **timestamp:** DateTime64[cite: 10]
* **metadata:** Map(String, String)[cite: 10]

## **Getting Started**

### **1. Launch Stack**
```bash
docker compose up -d