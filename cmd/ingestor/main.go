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

	pb "lumenlog/proto/gen"
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
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	producer = p

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

	// Start HTTP server for Sentinel Proxy events
	go func() {
		http.HandleFunc("/events", handleEvents)
		fmt.Println("Ingestor listening on :9001 for Sentinel Proxy events...")
		if err := http.ListenAndServe(":9001", nil); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	fmt.Println("Lumen Ingestor Live! Processing logs...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// --- BATCHING LOGIC ---
	const batchSize = 1 // Set to 1 for immediate dashboard updates
	var count int

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
	if err != nil {
		// Wait for ClickHouse to initialize the table if it's not ready
		for {
			batch, err = conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
			if err == nil {
				break
			}
			fmt.Println("Waiting for ClickHouse 'logs' table...")
			time.Sleep(2 * time.Second)
		}
	}

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Shutting down (%v). Flushing final logs...\n", sig)
			batch.Send()
			c.Close()
			producer.Flush(1000)
			producer.Close()
			return
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				logData := &pb.LogEvent{}
				if err := proto.Unmarshal(e.Value, logData); err != nil {
					fmt.Printf("Protobuf Decode Error: %v\n", err)
					continue
				}

				// Append log to ClickHouse batch
				err := batch.Append(
					logData.GetServiceName(),
					logData.GetHost(),
					logData.GetLevel(),
					logData.GetMessage(),
					logData.GetUserId(),
					time.Unix(0, logData.GetTimestamp()),
					fmt.Sprintf("%v", logData.GetMetadata()),
				)
				if err != nil {
					fmt.Printf("ClickHouse Append Error: %v\n", err)
					continue
				}

				count++
				if count >= batchSize {
					if err := batch.Send(); err != nil {
						fmt.Printf("ClickHouse Batch Send Error: %v\n", err)
					}

					// Re-prepare batch for next set of logs
					batch, _ = conn.PrepareBatch(ctx, "INSERT INTO lumen_db.logs")
					count = 0
				}

			case kafka.Error:
				fmt.Printf("Kafka Consumer Error: %v\n", e)
			}
		}
	}
}

// handleEvents processes incoming alerts from proxy.go
func handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Extract the User Identity (e.g., "bob") and the Security event
	userId := fmt.Sprintf("%v", event["user_id"])
	attackType := fmt.Sprintf("%v", event["attack_type"])

	logData := &pb.LogEvent{
		ServiceName: "sentinel-proxy",
		UserId:      userId,
		Message:     fmt.Sprintf("Security Event: %s detected for user %s", attackType, userId),
		Level:       "INFO",
		Timestamp:   time.Now().UnixNano(),
	}

	// If a real attack is identified, upgrade the log level to SECURITY
	if attackType != "" && attackType != "none" && attackType != "<nil>" {
		logData.Level = "SECURITY"
	}

	// Marshal and push to Redpanda (Kafka)
	payload, _ := proto.Marshal(logData)
	topic := "logs-raw"

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          payload,
	}, nil)

	if err != nil {
		fmt.Printf("Kafka Production Error: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
