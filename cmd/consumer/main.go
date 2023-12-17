package main

import (
	"log"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-kafka-1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		slog.Error("failed to create new consumer", err)
	}

	topics := []string{"teste"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			slog.Error("failed to read message", err)
			continue
		}

		log.Println(msg.String(), msg.TopicPartition)
	}
}
