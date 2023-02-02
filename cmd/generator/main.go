package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	index := 0
	for {
		index++
		produce([]byte(fmt.Sprintf("Message: %d\n", index)), "nfe")
		fmt.Printf("Message: %d", index)
	}
}

func produce(message []byte, topic string) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "host.docker.internal:9094",
	}

	kafkaProducer, err := kafka.NewProducer(configMap)

	if err != nil {
		panic(err)
	}

	kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)
}
