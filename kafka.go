package main

import (
	"encoding/json"
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"time"
)

func consumer(consumer *kafka.Consumer, out chan<- *Message, quit <-chan chan struct{}) {
	defer consumer.Close()

	for {
		select {
		case ack := <-quit:
			fmt.Printf("Closing consumer\n")
			ack <- struct{}{}
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err == nil {
				dataPoint := DataPoint{}

				if err := json.Unmarshal(msg.Value, &dataPoint); err != nil {
					fmt.Printf("Invalid data point discarded")
				} else {
					// TODO: which type of message is this one?
					out <- &Message{DataPoint: dataPoint, RawMessage: msg.Value}
				}
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}
}

func producer(producer *kafka.Producer, topic string, in <-chan *MergedMessage, quit <-chan chan struct{}) {
	defer producer.Close()

	for {
		select {
		case ack := <-quit:
			fmt.Printf("Closing producer\n")
			ack <- struct{}{}

			return
		case message := <-in:
			marshalledMessage, err := json.Marshal(message)

			if err != nil {
				fmt.Printf("Invalid merged message, can't marshal")
				continue
			}

			// TODO: haven't checked how to produce on partition based on key
			producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          marshalledMessage,
			}, nil)

			producer.Flush(15 * 1000)
		}
	}
}

func createConsumer(topics []string) *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka",
		"group.id":          "stream-merger-local",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	consumer.SubscribeTopics(topics, nil)
	return consumer
}

func createProducer() *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"linger.ms":         10,
		"acks":              "all",
	})

	if err != nil {
		panic(err)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return producer
}
