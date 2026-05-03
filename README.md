# LumenLog Distributed Log Ingestor Rust to Redpanda to Go to ClickHouse

## Category
<<<<<<< HEAD
Distributed Systems

A high-performance observability pipeline demonstrating cross-language data serialization and high-throughput persistence.
=======

Distributed Systems

A high performance observability pipeline demonstrating cross language data serialization and high throughput persistence.
>>>>>>> 9a4765e (Complete end to end pipeline with Rust agent, Redpanda, Go ingestor, and ClickHouse storage)

## Architecture

* Producer Rust serializes log events using Protobuf and streams to Redpanda
* Broker Redpanda Kafka compatible message broker for high speed streaming
* Ingestor Go consumes binary data decodes via Protobuf and batches writes to ClickHouse
* Storage ClickHouse columnar database optimized for real time analytical queries

## Data Flow

Rust Agent generates structured logs
Logs are serialized with Protobuf
Sent to Redpanda topic logs raw
Go Ingestor consumes messages continuously
Messages are decoded and batched
Inserted into ClickHouse lumen_db logs table

## Current Features

* End to end pipeline fully working
* Real time ingestion from Kafka compatible broker
* Protobuf based schema for efficient transport
* Batched inserts into ClickHouse for performance
* Docker based infrastructure setup
* Continuous log streaming with visible ingestion

## Database Schema

Table lumen_db logs

Columns

* service_name String
* host String
* level String
* message String
* timestamp DateTime64
* metadata String

## Getting Started
<<<<<<< HEAD
1. **Infrastructure:**
   ```bash
   docker-compose up -d
2. **Setup Table:**
    Run the SQL found in clickhouse_setup.sql (or via docker-exec) to create the lumen_db.logs table.
3. **Run Ingestor (Go):**
    cd cmd/ingestor && go run main.go
4. **Run Agent (Rust):**
    Requires Rust 1.78+ (Edition 2021) cd agent && cargo run
=======

### 1 Infrastructure

```bash
docker compose up -d
```

### 2 Verify ClickHouse

```bash
docker exec -it clickhouse clickhouse-client
```

Then:

```sql
SHOW DATABASES;
USE lumen_db;
SHOW TABLES;
SELECT count(*) FROM logs;
```

### 3 Run Ingestor

```bash
docker compose up ingestor
```

You should see:
Consumed message from Kafka
Batched logs to ClickHouse

### 4 Run Rust Agent

```bash
cd agent
cargo run
```

## Example Query

```sql
SELECT * FROM logs ORDER BY timestamp DESC LIMIT 10;
```

## Notes

* ClickHouse runs inside Docker no local db file is created
* Data persists inside container volumes
* Redpanda replaces Kafka for simpler local setup
* System is designed for high throughput ingestion not transactional workloads

## Status

Working end to end pipeline with real data ingestion
>>>>>>> 9a4765e (Complete end to end pipeline with Rust agent, Redpanda, Go ingestor, and ClickHouse storage)
