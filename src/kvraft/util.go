package kvraft

import "log"

const Debug = 0
const lock_trace = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf("KV server: "+format, a...)
	}
	return
}

func DPrint(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Print(a...)
	}
	return
}
