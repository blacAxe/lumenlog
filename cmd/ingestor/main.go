package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"

	pb "github.com/omar/lumenlog/proto/gen" //
)

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

	// Setup Kafka Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "lumen-ingestor-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Kafka consumer failed: %v", err)
	}

	c.SubscribeTopics([]string{"logs-raw"}, nil)
	fmt.Println("Go Ingestor Live! Batching logs to ClickHouse...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// --- BATCHING LOGIC ---
	const batchSize = 100
	var count int

	// Create a new batch
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO logs")
	if err != nil {
		log.Fatal(err)
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
					time.Unix(0, logData.Timestamp), // Convert nanoseconds to time
					logData.Metadata,
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
					
					// Re-initialize the batch for next round
					batch, _ = conn.PrepareBatch(ctx, "INSERT INTO logs")
					count = 0
				}

			case kafka.Error:
				fmt.Printf("%% Kafka Error: %v\n", e)
			}
		}
	}
}