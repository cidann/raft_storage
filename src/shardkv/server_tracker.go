package shardkv

import (
	"dsys/raft_helper"
)

type RequestTracker struct {
	Latest_applied map[int]int
	Request_serial map[int]int
	request_chan   map[int]chan any
}

func NewRequestTracker() *RequestTracker {
	tracker := RequestTracker{}
	tracker.Latest_applied = make(map[int]int)
	tracker.Request_serial = make(map[int]int)
	tracker.request_chan = make(map[int]chan any)

	return &tracker
}

func (tracker *RequestTracker) RecordRequest(operation raft_helper.Op, req_chan chan any) {
	Debug(dWarn, "DiscardRequestFrom sid:%d serial:%d", operation.Get_sid(), operation.Get_serial())
	tracker.DiscardRequestFrom(operation.Get_sid())
	tracker.Request_serial[operation.Get_sid()] = operation.Get_serial()
	tracker.request_chan[operation.Get_sid()] = req_chan
}

func (tracker *RequestTracker) AlreadyProcessed(operation raft_helper.Op) bool {
	already_processed := false
	if serial, ok := tracker.Latest_applied[operation.Get_sid()]; ok && serial >= operation.Get_serial() {
		already_processed = true
		Debug(dWarn, "AlreadyProcessed! sid:%d serial:%d old latest serial: %d", operation.Get_sid(), operation.Get_serial(), serial)
	}
	return already_processed
}

func (tracker *RequestTracker) ProcessRequest(operation raft_helper.Op, result any) {
	if !tracker.AlreadyProcessed(operation) {
		tracker.Latest_applied[operation.Get_sid()] = operation.Get_serial()
	}

	if tracker.request_chan[operation.Get_sid()] != nil && operation.Get_serial() == tracker.Request_serial[operation.Get_sid()] {
		tracker.request_chan[operation.Get_sid()] <- result
		Debug(dWarn, "DiscardRequestFrom sid:%d serial:%d", operation.Get_sid(), operation.Get_serial())
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

func (tracker *RequestTracker) DiscardAll() {
	Assert(tracker.request_chan != nil, "Discard nil chan")
	Debug(dWarn, "Discard All")
	for k := range tracker.request_chan {
		tracker.DiscardRequestFrom(k)
	}
}

func (tracker *RequestTracker) Copy() *RequestTracker {
	return &RequestTracker{
		Latest_applied: CopyMap[int, int](tracker.Latest_applied),
		Request_serial: CopyMap[int, int](tracker.Request_serial),
		request_chan:   CopyMap[int, chan any](tracker.request_chan),
	}
}

/*
func (tracker *RequestTracker) Lock() {
	tracker.mu.Lock()
}

func (tracker *RequestTracker) Unlock() {
	tracker.mu.Unlock()
}

func (tracker *RequestTracker) Identity() string {
	return "RequestTracker"
}
*/
