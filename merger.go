package main

import (
	"fmt"
	"sync"
)

type struct State {
	storage map[string]*MergedMessage
}

func (s *State) getForDeviceId(deviceId string) : MergedMessage {
	return storage[deviceId]
}


func merger(in <-chan *Message, out chan<- *MergedMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	state := make(map[string]*MergedMessage)

	for {
		msg := <-in

		fmt.Printf("Message for device %v on T %v. Raw message: %s\n", msg.DataPoint.DeviceId, msg.DataPoint.Time, string(msg.RawMessage))

		messageState := state[msg.DataPoint.DeviceId]

		// No previous state. Let's create the state.
		if (messageState == nil) {
			rawMessages := make([][]byte, 1)
			rawMessages[0] = msg.RawMessage

			state[msg.DataPoint.DeviceId] = createEmptyState(msg.DataPoint, msg.RawMessage)
			continue
		}

		// If the new data point is not for the same time let's flush the previous data and create the new state
		// TODO: stract to methods?
		if (msg.DataPoint.Time != messageState.DataPoint.Time) {
			out <- messageState
			state[msg.DataPoint.DeviceId] = nil
			state[msg.DataPoint.DeviceId] = createEmptyState(msg.DataPoint, msg.RawMessage)
			continue
		}

		// In all other cases we are merging a new message
		rawMessages := append(state[msg.DataPoint.DeviceId].RawMessages, msg.RawMessage)
		state[msg.DataPoint.DeviceId].RawMessages = rawMessages
	}
}

func createEmptyState(dataPoint DataPoint, msg []byte) *MergedMessage {
	rawMessages := make([][]byte, 1)
	rawMessages[0] = msg

	return &MergedMessage{DataPoint: dataPoint, RawMessages: rawMessages}
}