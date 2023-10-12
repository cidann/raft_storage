package raft

import "time"

type RaftTimer struct {
	LastRecord time.Time
}

func (rtimer *RaftTimer) RecordTime() {
	rtimer.LastRecord = time.Now()
}

func (rtimer *RaftTimer) HasElapsed(duration time.Duration) bool {
	return time.Since(rtimer.LastRecord) >= duration
}

func (rf *Raft) UnlockAndSleepFor(d time.Duration) {
	rf.mu.Unlock()
	time.Sleep(d)
	rf.mu.Lock()
}
