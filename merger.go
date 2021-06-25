package main

import (
	"sync"
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

func merger(in <-chan *Message, out chan<- *MergedMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	state := NewState()

	for {
		msg := <-in

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
