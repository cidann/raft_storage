package kvraft

type RequestTracker struct {
	latest_applied map[int]int
	request_serial map[int]int
	request_chan   map[int]chan string
}

func NewRequestTracker() *RequestTracker {
	tracker := RequestTracker{}
	tracker.latest_applied = make(map[int]int)
	tracker.request_serial = make(map[int]int)
	tracker.request_chan = make(map[int]chan string)

	return &tracker
}

func (tracker *RequestTracker) RecordRequest(operation *Op, req_chan chan string) {
	tracker.DiscardRequestFrom(operation.Sid)
	tracker.request_serial[operation.Sid] = operation.Serial
	tracker.request_chan[operation.Sid] = req_chan
}

func (tracker *RequestTracker) AlreadyProcessed(operation *Op) bool {
	already_processed := false
	if serial, ok := tracker.latest_applied[operation.Sid]; ok && (serial+1) > operation.Serial {
		already_processed = true
	}
	return already_processed
}

func (tracker *RequestTracker) ProcessRequest(operation *Op, result string) {
	tracker.latest_applied[operation.Sid] = operation.Serial

	if tracker.request_chan[operation.Sid] != nil && operation.Serial == tracker.request_serial[operation.Sid] {
		tracker.request_chan[operation.Sid] <- result
		tracker.DiscardRequestFrom(operation.Sid)
	}
}

func (tracker *RequestTracker) DiscardRequestFrom(sid int) {
	if tracker.request_chan[sid] != nil {
		close(tracker.request_chan[sid])
		tracker.request_chan[sid] = nil
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
