package shardmaster

import "dsys/raft_helper"

type RequestTracker struct {
	latest_applied map[int]int
	request_serial map[int]int
	request_chan   map[int]chan Config
}

func NewRequestTracker() *RequestTracker {
	tracker := RequestTracker{}
	tracker.latest_applied = make(map[int]int)
	tracker.request_serial = make(map[int]int)
	tracker.request_chan = make(map[int]chan Config)

	return &tracker
}

func (tracker *RequestTracker) RecordRequest(operation raft_helper.Op, req_chan chan Config) {
	tracker.DiscardRequestFrom(operation.Get_sid())
	tracker.request_serial[operation.Get_sid()] = operation.Get_serial()
	tracker.request_chan[operation.Get_sid()] = req_chan
}

func (tracker *RequestTracker) AlreadyProcessed(operation raft_helper.Op) bool {
	already_processed := false
	if serial, ok := tracker.latest_applied[operation.Get_sid()]; ok && serial >= operation.Get_serial() {
		already_processed = true
	}
	return already_processed
}

func (tracker *RequestTracker) ProcessRequest(operation raft_helper.Op, result Config) {
	if !tracker.AlreadyProcessed(operation) {
		tracker.latest_applied[operation.Get_sid()] = operation.Get_serial()
	}

	if tracker.request_chan[operation.Get_sid()] != nil && operation.Get_serial() == tracker.request_serial[operation.Get_sid()] {
		tracker.request_chan[operation.Get_sid()] <- result
		tracker.DiscardRequestFrom(operation.Get_sid())
	}
}

func (tracker *RequestTracker) DiscardRequestFrom(sid int) {
	if tracker.request_chan[sid] != nil {
		Debug(dWarn, "debug close chan %d", sid)
		close(tracker.request_chan[sid])
		tracker.request_chan[sid] = nil
	}
}
