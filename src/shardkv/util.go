package shardkv

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const DEBUG = 0
const lock_trace = false

var debugStart time.Time
var debugVerbosity int
var debugShardOnly bool = false

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
	dConf    logTopic = "CONF"
	dTrans   logTopic = "TRAN"
	dDECI    logTopic = "DECI"
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
		if !debugShardOnly || (topic == dConf || topic == dTrans || topic == dDECI) {
			time := time.Since(debugStart).Microseconds()
			time /= 100
			prefix := fmt.Sprintf("%06d %v ", time, string(topic))
			format = prefix + format
			log.Printf(format, a...)
		}
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

func RecursiveToString(data any) string {
	res, err := json.MarshalIndent(data, "", "\t")
	Assert(err == nil, "RecursiveToString failed")
	return string(res)
}

type AtomicMap[K comparable, V any] struct {
	mu        *sync.Mutex
	inner_map map[K]V
}

func NewAtomicMap[K comparable, V any]() *AtomicMap[K, V] {
	return &AtomicMap[K, V]{
		mu:        &sync.Mutex{},
		inner_map: map[K]V{},
	}
}

func (amap *AtomicMap[K, V]) Apply(f func(inner_map map[K]V) V) V {
	amap.mu.Lock()
	defer amap.mu.Unlock()
	return f(amap.inner_map)
}

/*
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if DEBUG > 0 {
		log.Printf("KV server: "+format, a...)
	}
	return
}
*/
