package raft

import "time"

type RaftTimer struct {
	LastRecord time.Time
	manual     bool
}

func (rtimer *RaftTimer) RecordTime() {
	rtimer.LastRecord = time.Now()
}

func (rtimer *RaftTimer) SetElapsed() {
	rtimer.manual = true
}

func (rtimer *RaftTimer) HasElapsed(duration time.Duration) bool {
	elapsed := time.Since(rtimer.LastRecord) >= duration
	if rtimer.manual {
		rtimer.manual = false
		elapsed = true
	}
	return elapsed
}
