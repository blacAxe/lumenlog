# LumenLog: Distributed Log Ingestor (Rust -> Redpanda -> Go -> ClickHouse)

A high-performance observability pipeline demonstrating cross-language data serialization and high-throughput persistence.

## Architecture
- *Producer (Rust):* Serializes log events using Protobuf and streams to Redpanda.
- *Broker (Redpanda):* A Kafka-compatible, C++ based message broker for high-speed streaming.
- *Ingestor (Go):* Consumes binary data, decodes via Protobuf, and batches writes to ClickHouse.
- *Storage (ClickHouse):* A columnar database optimized for real-time analytical queries.

## Getting Started
1. **Infrastructure:**
   ```bash
   docker-compose up -d
2. **Setup Table:**
    Run the SQL found in clickhouse_setup.sql (or via docker-exec) to create the lumen_db.logs table.
3. **Run Ingestor (Go):**
    cd cmd/ingestor && go run main.go
4. **Run Agent (Rust):**
    Requires Rust 1.78+ (Edition 2021) cd agent && cargo run