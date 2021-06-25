package main

import (
	"fmt"
	"sync"
)

type DataPoint struct {
	Time int `json:"measurement_time"`
	DeviceId string `json:"device_id"`
}

type Message struct {
	DataPoint DataPoint
	RawMessage []byte
}

type MergedMessage struct {
	DataPoint DataPoint
	RawMessages [][]byte
}

func main() {
	var wg sync.WaitGroup
	kafkaConsumer := createConsumer()
	kafkaProducer := createProducer()

    mergerChan := make(chan *Message, 100)
    mergerOutChan := make(chan *MergedMessage, 100)


    wg.Add(3)
	go consumer(kafkaConsumer, mergerChan, &wg)
	go merger(mergerChan, mergerOutChan, &wg)
	go producer(kafkaProducer, "output_merged", mergerOutChan, &wg)

	fmt.Printf("Running...\n")
	wg.Wait()
	fmt.Printf("Closing...\n")
}
