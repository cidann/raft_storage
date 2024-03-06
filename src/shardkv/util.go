package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const DEBUG = 0
const lock_trace = false

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
	dInit    logTopic = "INIT"
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

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// time logged is 1/10th of a milisecond
func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func Assert(is_valid bool, format string, a ...interface{}) {
	if !is_valid {
		panic(fmt.Sprintf(format, a...))
	}
}

func CopyMap[K comparable, V any](src map[K]V) map[K]V {
	dst := map[K]V{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func Transform[T, U any](src []T, unary func(T) U) []U {
	dst := make([]U, len(src))
	for i, v := range src {
		dst[i] = unary(v)
	}
	return dst
}

func TransformMap[T, U any, K comparable](src map[K]T, unary func(T) U) map[K]U {
	dst := make(map[K]U)
	for i, v := range src {
		dst[i] = unary(v)
	}
	return dst
}

func Flatten[T any](src [][]T) []T {
	dst := make([]T, len(src))
	for _, slice := range src {
		for _, v := range slice {
			dst = append(dst, v)
		}
	}
	return dst
}

func FlattenMap[T any, U comparable](src map[U][]T) []T {
	dst := make([]T, len(src))
	for _, slice := range src {
		for _, v := range slice {
			dst = append(dst, v)
		}
	}
	return dst
}

/*
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf("KV server: "+format, a...)
	}
	return
}
*/
