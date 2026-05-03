package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	pb "github.com/omar/lumenlog/proto/gen" //
)

var producer *kafka.Producer

func main() {
	ctx := context.Background()

	// Setup ClickHouse Connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
		Auth: clickhouse.Auth{
			Database: "lumen_db",
			Username: "default",
			Password: "lumenlog2026",
		},
	})

	if err != nil {
		log.Fatalf("ClickHouse connection failed: %v", err)
	}

	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ClickHouse not reachable: %v", err)
	}

	// Setup Kafka Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "redpanda:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	producer = p // Assign to global variable

	// Setup Kafka Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "redpanda:9092",
		"group.id":          "lumen-ingestor",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Kafka consumer failed: %v", err)
	}

	c.SubscribeTopics([]string{"logs-raw"}, nil)
	// Start HTTP server for Sentinel events in a goroutine
	go func() {
		http.HandleFunc("/events", handleEvents)
		fmt.Println("HTTP Server listening on :9001 for Sentinel events...")
		if err := http.ListenAndServe(":9001", nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	fmt.Println("Go Ingestor Live! Batching logs to ClickHouse...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// --- BATCHING LOGIC ---
	const batchSize = 1
	var count int

	// Create a new batch
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
	if err != nil {
		for {
			batch, err = conn.PrepareBatch(ctx, "INSERT INTO logs")
			if err == nil {
				break
			}
			fmt.Println("Waiting for ClickHouse table...")
			time.Sleep(2 * time.Second)
		}
	}

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Shutting down (%v). Sending final batch...\n", sig)
			batch.Send()
			c.Close()
			return
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				fmt.Println("Consumed message from Kafka")

				logData := &pb.LogEvent{}
				if err := proto.Unmarshal(e.Value, logData); err != nil {
					fmt.Printf("Failed to decode: %v\n", err)
					continue
				}

				// Append to the current batch
				err := batch.Append(
					logData.ServiceName,
					logData.Host,
					logData.Level,
					logData.Message,
					time.Unix(0, logData.Timestamp),     // Convert nanoseconds to time
					fmt.Sprintf("%v", logData.Metadata), // Convert map to string for storage
				)
				if err != nil {
					fmt.Printf("Batch append error: %v\n", err)
					continue
				}

				count++
				// When hitting the limit, send it and start a new batch
				if count >= batchSize {
					if err := batch.Send(); err != nil {
						fmt.Printf("Failed to send batch: %v\n", err)
					}
					fmt.Printf("Batched %d logs to ClickHouse\n", count)

					// Reinitialize the batch for next round
					batch, err = conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
					if err != nil {
						for {
							batch, err = conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
							if err == nil {
								break
							}
							fmt.Println("Waiting for ClickHouse table...")
							time.Sleep(2 * time.Second)
						}
					}
					count = 0
				}

			case kafka.Error:
				fmt.Printf("%% Kafka Error: %v\n", e)
			}
		}
	}
}

func handleEvents(w http.ResponseWriter, r *http.Request) {
	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Map JSON to Protobuf LogEvent
	logData := &pb.LogEvent{
		ServiceName: "sentinel-proxy",
		Host:        "sentinel-internal",
		Level:       "SECURITY",
		Message:     fmt.Sprintf("Action: %v | Path: %v | Attack: %v", event["action"], event["path"], event["attack_type"]),
		Timestamp:   time.Now().UnixNano(),
		Metadata:    make(map[string]string),
	}

	// Put raw details in Metadata
	logData.Metadata["ip"] = fmt.Sprintf("%v", event["ip"])
	logData.Metadata["request_id"] = fmt.Sprintf("%v", event["request_id"])

	// Serialize to binary Protobuf
	payload, _ := proto.Marshal(logData)

	// Push to Redpanda 'logs-raw'
	topic := "logs-raw"
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil)

	fmt.Printf("[PIPELINE] Pushed Sentinel event %s to Redpanda\n", event["request_id"])
	w.WriteHeader(http.StatusOK)
}
