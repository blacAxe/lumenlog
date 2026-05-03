package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	pb "github.com/omar/lumenlog/proto/gen"
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

	c.SubscribeTopics([]string{"logs-raw"}, nil)

	fmt.Println("Alerter Service Live! Monitoring for Security Events...")

	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			logData := &pb.LogEvent{}
			if err := proto.Unmarshal(e.Value, logData); err == nil {
				// ONLY alert if it's a SECURITY level log
				if logData.Level == "SECURITY" {
					sendToDiscord(logData)
				}
			}
		}
	}
}

func sendToDiscord(event *pb.LogEvent) {
	msg := map[string]string{
		"content": fmt.Sprintf("**SECURITY ALERT** \n**Service:** %s\n**Message:** %s\n**Time:** %s",
			event.ServiceName, event.Message, time.Unix(0, event.Timestamp).Format(time.RFC1123)),
	}
	body, _ := json.Marshal(msg)
	http.Post(discordWebhookURL, "application/json", bytes.NewBuffer(body))
	fmt.Println("Alert sent to Discord!")
}
