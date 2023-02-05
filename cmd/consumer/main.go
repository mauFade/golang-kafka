package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

func main() {

}

func Consume(topics []string, servers string, msgChannel chan *kafka.Message) {
	kafkaConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": servers,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	err = kafkaConsumer.SubscribeTopics(topics, nil)

	if err != nil {
		panic(err)
	}

	for {
		msg, err := kafkaConsumer.ReadMessage(-1)

		if err == nil {
			msgChannel <- msg
		}
	}
}
