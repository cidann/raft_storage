package raft

import "time"

func (rf *Raft) UnlockAndSleepFor(d time.Duration) {
	rf.mu.Unlock()
	time.Sleep(d)
	rf.mu.Lock()
}

func (rf *Raft) UnlockUntilAppliable(entry RaftEntry) {
	rf.mu.Unlock()
	defer rf.mu.Lock()
	if !rf.killed() {
		rf.applyCh <- entry.(ApplyMsg)
	}
}

func GetChanForFunc(f func()) chan RaftEntry {
	c := make(chan RaftEntry)
	go func() {
		f()
		c <- ApplyMsg{}
	}()
	return c
}

func GetChanForTime(d time.Duration) chan interface{} {
	c := make(chan interface{})
	go func() {
		time.Sleep(d)
		c <- ""
	}()
	return c
}

func WaitUntilChan(c chan RaftEntry) RaftEntry {
	for s := range c {
		return s
	}
	return ApplyMsg{}
}
