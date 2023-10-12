package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
type DebugScheduleMode int

const (
	NoDelay int = iota
	DefinedDelay
	RandomDelay
)

const Debug = 0
const DebugSchedule = DefinedDelay
const RPCAppendDelay = 100
const RPCVoteDelay = 100
const ElectionDelay = 2000
const CommitDelay = 2000

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfl2(format string, a ...interface{}) (n int, err error) {
	if Debug > -1 {
		log.Printf(format, a...)
	}
	return
}

func DelaySchedule(t int) {
	if DebugSchedule == NoDelay {
		return
	}
	if DebugSchedule == DefinedDelay {
		time.Sleep((time.Duration)(t) * time.Millisecond)
	}
	if DebugSchedule == RandomDelay {
		time.Sleep((time.Duration)(rand.Int()%t) * time.Millisecond)
	}
}
