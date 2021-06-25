package main

import (
	"bytes"
	"testing"
	"time"
)

func TestFlushesOneMessageOnTimestampChange(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MergedMessage)
	close := make(chan chan struct{})

	go merger(in, out, close)

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID1"}, RawMessage: []byte("Message 2")}

	output := <-out

	if output.DataPoint.Time != 1 || output.DataPoint.DeviceId != "ID1" {
		t.Fatal("DataPoint was not the expected one &DataPoint{1, ID1}")
	}

	if len(output.RawMessages) != 1 || !bytes.Equal(output.RawMessages[0], []byte("Message 1")) {
		t.Fatal("Unexpected merged message")
	}
}

func TestFlushesAggregatedeMessageOnTimestampChange(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MergedMessage)
	close := make(chan chan struct{})

	go merger(in, out, close)

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 2")}
	in <- &Message{DataPoint: DataPoint{Time: 3, DeviceId: "ID1"}, RawMessage: []byte("Message 3")}

	output := <-out

	if output.DataPoint.Time != 1 || output.DataPoint.DeviceId != "ID1" {
		t.Fatal("DataPoint was not the expected one &DataPoint{1, ID1}")
	}

	if len(output.RawMessages) != 2 {
		t.Fatal("Unexpected number of messages")
	}
}

func TestNewDeviceDoesntFlushExistingOnes(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MergedMessage)
	close := make(chan chan struct{})

	go merger(in, out, close)

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID2"}, RawMessage: []byte("Message 2")}

	select {
	case <-out:
		t.Fatal("Output was not expected")
	case <-time.After(1 * time.Second):
	}
}

func TestMultipleFlushes(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MergedMessage)
	close := make(chan chan struct{})

	go merger(in, out, close)

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID2"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 2")}

	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}

	output := <-out
	if output.DataPoint.Time != 1 || output.DataPoint.DeviceId != "ID1" {
		t.Fatal("DataPoint was not the expected one &DataPoint{1, ID1}")
	}

	if len(output.RawMessages) != 2 {
		t.Fatal("Unexpected number of messages")
	}

	in <- &Message{DataPoint: DataPoint{Time: 3, DeviceId: "ID2"}, RawMessage: []byte("Message 2")}
	output = <-out

	if output.DataPoint.Time != 2 || output.DataPoint.DeviceId != "ID2" {
		t.Fatal("DataPoint was not the expected one &DataPoint{2, ID2}")
	}

	if len(output.RawMessages) != 1 {
		t.Fatal("Unexpected number of messages")
	}

	select {
	case <-out:
		t.Fatal("More messages where not expected")
	case <-time.After(1 * time.Second):
	}

}

func TestFlushesOnClose(t *testing.T) {
	in := make(chan *Message)
	out := make(chan *MergedMessage, 5)
	close := make(chan chan struct{})

	go merger(in, out, close)

	in <- &Message{DataPoint: DataPoint{Time: 1, DeviceId: "ID1"}, RawMessage: []byte("Message 1")}
	in <- &Message{DataPoint: DataPoint{Time: 2, DeviceId: "ID2"}, RawMessage: []byte("Message 1")}

	ackChan := make(chan struct{})
	close <- ackChan

	<-ackChan

	output := <-out
	if output.DataPoint.Time != 1 || output.DataPoint.DeviceId != "ID1" {
		t.Fatal("DataPoint was not the expected one &DataPoint{1, ID1}")
	}

	if len(output.RawMessages) != 1 {
		t.Fatal("Unexpected number of messages")
	}
	output = <-out

	if output.DataPoint.Time != 2 || output.DataPoint.DeviceId != "ID2" {
		t.Fatal("DataPoint was not the expected one &DataPoint{2, ID2}")
	}

	if len(output.RawMessages) != 1 {
		t.Fatal("Unexpected number of messages")
	}

	select {
	case <-out:
		t.Fatal("More messages where not expected")
	case <-time.After(1 * time.Second):
	}

}
