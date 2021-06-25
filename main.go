package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type DataPoint struct {
	Time     int    `json:"measurement_time"`
	DeviceId string `json:"device_id"`
}

type Message struct {
	DataPoint  DataPoint
	RawMessage []byte
}

type MessageTuple struct {
	Key      string
	Messages [][]byte
}

func main() {
	var wg sync.WaitGroup
	kafkaConsumer := createConsumer([]string{"temperature", "position", "power"})
	kafkaProducer := createProducer()

	mergerChan := make(chan *Message, 100)
	mergerOutChan := make(chan *MessageTuple, 100)

	consumerClose := make(chan chan struct{})
	mergerClose := make(chan chan struct{})
	producerClose := make(chan chan struct{})
	wg.Add(3)

	go signalHandling(consumerClose, mergerClose, producerClose, &wg)
	go consumer(kafkaConsumer, mergerChan, consumerClose)
	go merger(mergerChan, mergerOutChan, mergerClose, currentBucket)
	go producer(kafkaProducer, "output_merged", mergerOutChan, producerClose)

	fmt.Printf("Running...\n")
	wg.Wait()
	fmt.Printf("Closing...\n")
}

func signalHandling(
	consumerClose chan<- chan struct{},
	mergerClose chan<- chan struct{},
	producerClose chan<- chan struct{},
	wg *sync.WaitGroup) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		// Normal shutdown
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,

		// Stack dump shutdown
		syscall.SIGQUIT,
		syscall.SIGILL,
		syscall.SIGTRAP,
		syscall.SIGABRT,
		syscall.SIGEMT,
		syscall.SIGSYS,
	)

	signal := <-sigc
	fmt.Printf("Got signal %s starting ordered shutdown\n", signal)

	waitChan := make(chan struct{})

	consumerClose <- waitChan
	<-waitChan
	wg.Done()

	mergerClose <- waitChan
	<-waitChan
	wg.Done()

	producerClose <- waitChan
	<-waitChan
	wg.Done()

	fmt.Printf("Done closing\n")
}
