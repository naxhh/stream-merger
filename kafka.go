package main

import (
	"fmt"
	"sync"
	"encoding/json"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)


func consumer(consumer *kafka.Consumer, out chan<- *Message, wg *sync.WaitGroup) {
	defer wg.Done()
	defer consumer.Close()

	// TODO: move to its own procedure
	// TODO: close on signal
	for {
		fmt.Printf("Waiting for message\n")
		msg, err := consumer.ReadMessage(-1)
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

func producer(producer *kafka.Producer, topic string, in <-chan *MergedMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	defer producer.Close()
	
	for {
		message := <-in
		marshalledMessage, err := json.Marshal(message)

	    if err != nil {
    		fmt.Printf("Invalid merged message, can't marshal")
	    	continue
	    }

		// TODO: haven't checked how to produce on partition based on key
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value: marshalledMessage,
		}, nil)

		producer.Flush(15 * 1000)
	}
}

func createConsumer() *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka",
		"group.id":          "stream-merger-local",
		"auto.offset.reset": "earliest",
		// TODO: review other options
	})

	if err != nil {
		panic(err)
	}

	// TODO: "position", "temperature" ,"power"
	consumer.SubscribeTopics([]string{"temperature"}, nil)
	return consumer
}

func createProducer() *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
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