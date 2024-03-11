package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var debugStart time.Time
var debugVerbosity int

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

// set verbosity from upper layer
func SetRaftMute(rf *Raft, mute bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.mute = mute
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(rf *Raft, topic logTopic, format string, a ...interface{}) {
	if !rf.mute && debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// Debugging
type DebugScheduleMode int

const (
	NoDelay int = iota
	DefinedDelay
	RandomDelay
)

const DEBUG = 0
const DebugSchedule = NoDelay
const RPCAppendDelay = 100
const RPCVoteDelay = 100
const ElectionDelay = 2000
const CommitDelay = 2000

var lock_trace bool

func getLockTrace() bool {
	v := os.Getenv("LT")
	lt := false
	if v != "" {
		lt = true
	}
	return lt
}

func init() {
	lock_trace = getLockTrace()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintfl2(format string, a ...interface{}) (n int, err error) {
	if DEBUG > -1 {
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

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
