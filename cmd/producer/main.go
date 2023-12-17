package main

import (
	"log"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	deliveryChann := make(chan kafka.Event)

	producer := NewKafkaProducer()
	Publish("ihhull", "teste", producer, nil, deliveryChann)

	go DeliveryReport(deliveryChann)

	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-kafka-1:9092",
	}

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	return producer
}

func Publish(msg, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for event := range deliveryChan {
		switch e := event.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				slog.Error("Failed to send message")
				continue
			}

			slog.Info("message sent", "response", e.TopicPartition)
		}
	}
}
