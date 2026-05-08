package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	pb "lumenlog/proto/gen"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"google.golang.org/protobuf/proto"
)

const discordWebhookURL = "https://discord.com/api/webhooks/1500519975360663584/io7M4tjtD20AqG9l7WiA0_GIX3T-VsCTt4DvL679mOTHyBhVHVuoUW9iTm7Ff-T4Zl-Q"

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "redpanda:9092",
		"group.id":          "lumen-alerter",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal(err)
	}

	// SUBSCRIBE TO BOTH TOPICS
	// 'logs-raw' for standard app logs, 'security-events' for your Rust Agent
	err = c.SubscribeTopics([]string{"logs-raw", "security-events"}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics: %v", err)
	}

	fmt.Println("🚀 Alerter Service Live! Monitoring for Security Events...")

	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			logData := &pb.LogEvent{}

			// UNMARSHAL PROTOBUF
			// This is where the Rust binary data becomes a Go struct
			err := proto.Unmarshal(e.Value, logData)
			if err != nil {
				log.Printf("Error decoding message from %s: %v", *e.TopicPartition.Topic, err)
				continue
			}

			// BROAD ALERT LOGIC
			// Fire if it's from the security-events topic OR if the level is SECURITY
			if *e.TopicPartition.Topic == "security-events" || logData.GetLevel() == "SECURITY" || logData.GetAttackType() != "" {
				log.Printf("🚨 Security Event Detected for User: %s", logData.GetUserId())
				sendToDiscord(logData)
			}
		}
	}
}

func sendToDiscord(event *pb.LogEvent) {
	// Fallback for user if it's empty
	user := event.GetUserId()
	if user == "" {
		user = "anonymous"
	}

	// Format timestamp correctly (handling both Unix seconds and nanoseconds)
	ts := event.GetTimestamp()
	var timeStr string
	if ts > 1000000000000 { // Likely nanoseconds
		timeStr = time.Unix(0, ts).Format(time.RFC1123)
	} else {
		timeStr = time.Unix(ts, 0).Format(time.RFC1123)
	}

	msg := map[string]string{
		"content": fmt.Sprintf("🚨 **SECURITY ALERT**\n**User:** `%s`\n**Service:** `%s`\n**Attack:** %s\n**Action:** %s\n**Time:** %s\n**Message:** %s",
			user,
			event.GetServiceName(),
			event.GetAttackType(),
			event.GetAction(),
			timeStr,
			event.GetMessage()),
	}

	body, _ := json.Marshal(msg)
	resp, err := http.Post(discordWebhookURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to ship to Discord: %v", err)
		return
	}
	defer resp.Body.Close()
}
