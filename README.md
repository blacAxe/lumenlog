# **LumenLog: Distributed Polyglot Observability Pipeline**

## **Category**
Distributed Systems / Security Engineering

A high-performance observability pipeline demonstrating cross-language data serialization and high-throughput persistence. This system serves as the central nervous system for unified logging across Go and Rust environments.

## **Architecture**

* **Producer (Rust):** Serializes system heartbeats using Protobuf and streams to Redpanda
* **Producer (Go):** Sentinel WAF bridges security events via JSON-to-Protobuf translation
* **Broker (Redpanda):** Kafka-compatible message broker for high-speed event streaming
* **Ingestor (Go):** Consumes binary data, decodes via Protobuf, and batches writes to ClickHouse
* **Alerter (Go):** Real-time sidecar service that triggers Discord notifications for security events
* **Storage (ClickHouse):** Columnar database optimized for real-time analytical queries

## **Data Flow**

1. **Rust Agent** generates system logs -> Serialized with Protobuf -> Redpanda
2. **Sentinel Proxy** generates security events -> Bridged to Ingestor -> Redpanda
3. **Go Ingestor** consumes messages -> Decodes and batches -> ClickHouse
4. **Go Alerter** monitors stream -> Filters for "SECURITY" level -> Discord API

## **Core Features**

* **End-to-End Pipeline:** Fully integrated cross-language data flow
* **Unified Schema:** Shared Protobuf definitions for Rust and Go services
* **Active Response:** Real-time Discord alerting for critical security blocks
* **High-Throughput Persistence:** Batched inserts into ClickHouse for analytical scale
* **Containerized Infrastructure:** Entire stack managed via Docker Compose

## **Latest Update: The Security Bridge**

This update integrates **Sentinel Proxy** into the pipeline, transforming LumenLog into a Security Operations Platform.

* Added **Alerter Service** to handle real-time notifications
* Implemented **Discord Webhook** integration for instant threat visibility
* Unified system health (Rust) and security data (Go) into a single ClickHouse schema

## **Database Schema (lumen_db.logs)**

* **service_name:** String (e.g., 'rust-agent', 'sentinel-proxy')
* **host:** String
* **level:** String (INFO, WARN, SECURITY)
* **message:** String
* **timestamp:** DateTime64
* **metadata:** Map(String, String)

## **Getting Started**

### **1. Launch Stack**
```bash
docker compose up -d

---

```text
feat: implement real-time security alerting and unified log ingestion

- Add Alerter service for real-time Discord notifications
- Integrate Sentinel WAF security events into the Redpanda stream
- Refactor ingestor to handle multi-source Protobuf data
- Update Docker Compose to include the alerting sidecar
- Expand documentation to cover polyglot architecture and data flow