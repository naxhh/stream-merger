package main

import (
	"bytes"
	"testing"
	"time"
)

func TestAggregatesMessagesForSameDevice(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MessageTuple)
	close := make(chan chan struct{})

	var bucketId int64
	bucketId = 1

	go merger(in, out, close, func() int64 { return bucketId })

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID1"}, RawMessage: []byte("Message 2")}

	bucketId = 2
	output := <-out

	if output.Key != "ID1" {
		t.Fatal("DataPoint was not the expected one &DataPoint{1, ID1}")
	}

	if len(output.Messages) != 2 || !bytes.Equal(output.Messages[0], []byte("Message 1")) {
		t.Fatalf("Unexpected merged message got %s", output.Messages[0])
	}
}

func TestAggregatesMessagesForEachDevice(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MessageTuple)
	close := make(chan chan struct{})

	var bucketId int64
	bucketId = 1

	go merger(in, out, close, func() int64 { return bucketId })

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID2"}, RawMessage: []byte("Message 2")}
	in <- &Message{DataPoint: DataPoint{Time: 3, DeviceId: "ID1"}, RawMessage: []byte("Message 3")}
	in <- &Message{DataPoint: DataPoint{Time: 4, DeviceId: "ID2"}, RawMessage: []byte("Message 4")}

	bucketId = 2
	output := <-out

	if output.Key != "ID1" {
		t.Fatal("DataPoint was not the expected one")
	}

	if len(output.Messages) != 2 || !bytes.Equal(output.Messages[0], []byte("Message 1")) {
		t.Fatalf("Unexpected merged message got %s", output.Messages[0])
	}

	output = <-out

	if output.Key != "ID2" {
		t.Fatal("DataPoint was not the expected one")
	}

	if len(output.Messages) != 2 || !bytes.Equal(output.Messages[0], []byte("Message 2")) {
		t.Fatalf("Unexpected merged message got %s", output.Messages[0])
	}
}

func TestFlushesOnClose(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MessageTuple, 5)
	close := make(chan chan struct{})

	var bucketId int64
	bucketId = 1

	go merger(in, out, close, func() int64 { return bucketId })

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID2"}, RawMessage: []byte("Message 1")}

	ackChan := make(chan struct{})
	close <- ackChan

	select {
	case <-ackChan:
	case <-time.After(5 * time.Second):
		t.Fatal("Never received an ACK")
	}

	output := <-out
	if output.Key != "ID1" {
		t.Fatal("DataPoint was not the expected one")
	}

	if len(output.Messages) != 1 {
		t.Fatal("Unexpected number of messages")
	}

	output = <-out
	if output.Key != "ID2" {
		t.Fatal("DataPoint was not the expected one")
	}

	if len(output.Messages) != 1 {
		t.Fatal("Unexpected number of messages")
	}

	select {
	case <-out:
		t.Fatal("More messages where not expected")
	case <-time.After(1 * time.Second):
	}

}
