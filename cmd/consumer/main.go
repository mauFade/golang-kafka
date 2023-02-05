package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

func main() {
	msgChan := make(chan *kafka.Message)
	topics := []string{"nfe"}
	servers := "host.docker.internal: 9094"

	// Gera uma thread nova  e fica lendo dados do kafka
	go Consume(topics, servers, msgChan)

	for {
		msg := <-msgChan
		println(string(msg.Value))
	}
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
