package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewOrderPlacer(producer *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:     producer,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 10000),
	}
}

type OrderPlacer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

func (op *OrderPlacer) PlaceOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	topicPartition := kafka.TopicPartition{
		Topic:     &op.topic,
		Partition: kafka.PartitionAny,
	}

	kafkaMsg := &kafka.Message{
		TopicPartition: topicPartition,
		Value:          payload,
	}

	err := op.producer.Produce(kafkaMsg, op.deliveryChan)
	if err != nil {
		return err
	}

	fmt.Println("placed order on the queue: ", format)
	<-op.deliveryChan
	return nil
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "go-producer-1",
		"acks":              "all",
	})
	if err != nil {
		log.Fatal(err)
	}

	items := []string{"Waffer", "Coke", "Pepsi", "Fanta"}
	op := NewOrderPlacer(p, "HVSE")
	for qty := 0; qty < 10000; qty++ {
		item := items[time.Now().Unix()%4]

		if err := op.PlaceOrder(item, int(qty)); err != nil {
			log.Fatal(err)
		}

		time.Sleep(3 * time.Second)
	}
}
