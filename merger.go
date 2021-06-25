package main

import (
	"fmt"
)

type State struct {
	storage map[string]*MergedMessage
}

func NewState() *State {
	storage := make(map[string]*MergedMessage)

	return &State{storage: storage}
}

func (s *State) initialize(deviceId string, dataPoint DataPoint, msg []byte) {
	rawMessages := make([][]byte, 1)
	rawMessages[0] = msg

	s.storage[deviceId] = &MergedMessage{DataPoint: dataPoint, RawMessages: rawMessages}
}

func (s *State) get(deviceId string) *MergedMessage {
	return s.storage[deviceId]
}

func (s *State) appendMessage(deviceId string, msg []byte) {
	rawMessages := append(s.storage[deviceId].RawMessages, msg)
	s.storage[deviceId].RawMessages = rawMessages
}

func (s *State) getState() map[string]*MergedMessage {
	return s.storage
}

// TODO: aggregate the ordered messages into 1-minute batches which are sent to an output stream
// We are now just grouping until a new T comes in. We should change a bit the algo.
// We also will need some TTL checking for eficiency
func merger(in <-chan *Message, out chan<- *MergedMessage, quit <-chan chan struct{}) {
	state := NewState()

	for {
		select {
		case ack := <-quit:
			fmt.Printf("Flushing merger\n")

			for _, deviceState := range state.getState() {
				out <- deviceState
			}

			fmt.Printf("Closing merger\n")
			ack <- struct{}{}
			return

		case msg := <-in:

			deviceId := msg.DataPoint.DeviceId
			messageState := state.get(deviceId)

			// No previous state. Let's create the state.
			if messageState == nil {
				state.initialize(deviceId, msg.DataPoint, msg.RawMessage)
				continue
			}

			// If the new data point is not for the same time let's flush the previous data and create the new state
			if msg.DataPoint.Time != messageState.DataPoint.Time {
				out <- messageState
				state.initialize(deviceId, msg.DataPoint, msg.RawMessage)
				continue
			}

			// In all other cases we are merging a new message
			state.appendMessage(deviceId, msg.RawMessage)
		}
	}
}
