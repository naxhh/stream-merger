package main

import (
	"fmt"
	"time"
)

var currentBucket = func() int64 { return time.Now().Unix() / 60 }

type State struct {
	bucket          int64
	storage         map[string][][]byte
	out             chan<- *MessageTuple
	currentBucketFn func() int64
}

func NewState(out chan<- *MessageTuple, currentBucketFn func() int64) *State {
	bucket := currentBucketFn()
	storage := make(map[string][][]byte)

	return &State{storage: storage, bucket: bucket, out: out, currentBucketFn: currentBucketFn}
}

func (s *State) initialize(deviceId string, msg *Message) {
	s.storage[deviceId] = [][]byte{msg.RawMessage}
}

func (s *State) get(deviceId string) [][]byte {
	return s.storage[deviceId]
}

func (s *State) appendMessage(deviceId string, msg *Message) {
	messages := append(s.storage[deviceId], msg.RawMessage)
	s.storage[deviceId] = messages
}

func (s *State) bookkeeping() {
	maybeNewBucket := s.currentBucketFn()

	if s.bucket == maybeNewBucket {
		return
	}

	s.flush()
	s.bucket = maybeNewBucket
}

func (s *State) flush() {
	for key, messages := range s.storage {
		s.out <- &MessageTuple{Key: key, Messages: messages}
	}

	s.storage = make(map[string][][]byte)
}

func merger(in <-chan *Message, out chan<- *MessageTuple, quit <-chan chan struct{}, currentBucketFn func() int64) {
	state := NewState(out, currentBucketFn)

	for {
		select {
		case ack := <-quit:
			fmt.Printf("Flushing merger\n")

			state.flush()

			fmt.Printf("Closing merger\n")
			ack <- struct{}{}
			return

		case msg := <-in:

			deviceId := msg.DataPoint.DeviceId
			messageState := state.get(deviceId)

			// No previous state. Let's create it.
			if messageState == nil {
				state.initialize(deviceId, msg)
				state.bookkeeping()
				continue
			}

			state.appendMessage(deviceId, msg)
			state.bookkeeping()
		}
	}
}
