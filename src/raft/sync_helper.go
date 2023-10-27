package raft

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"
)

var max_stack_trace int = 2

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

func UnlockAndSleepFor(obj Lockable, d time.Duration) {
	Unlock(obj, lock_trace, "UnlockAndSleepFor")
	defer Lock(obj, lock_trace, "UnlockAndSleepFor")
	time.Sleep(d)
}

func UnlockUntilChanReceive[T any](obj Lockable, c chan T) T {
	Unlock(obj, lock_trace, "UnlockUntilChan")
	defer Lock(obj, lock_trace, "UnlockUntilChan")
	return WaitUntilChanReceive(c)
}

func UnlockUntilChanSend[T any](obj Lockable, c chan T) T {
	Unlock(obj, lock_trace, "UnlockUntilChan")
	defer Lock(obj, lock_trace, "UnlockUntilChan")
	return WaitUntilChanSend(c)
}

func GetChanForFunc[T any](f func()) chan T {
	c := make(chan T, 1)
	go func() {
		f()
		var zero_val T
		c <- zero_val
	}()
	return c
}

func GetChanForTime[T any](d time.Duration) chan T {
	c := make(chan T, 1)
	go func() {
		time.Sleep(d)
		var zero_val T
		c <- zero_val
	}()
	return c
}

func WaitUntilChanReceive[T any](c chan T) T {
	for s := range c {
		return s
	}
	var zero_val T
	return zero_val
}

func WaitUntilChanSend[T any](c chan T) T {
	val := <-c
	return val
}

type Lockable interface {
	Lock()
	Unlock()
	Identity() string
}

func Lock(obj Lockable, stack_trace bool, print_args ...interface{}) {
	if stack_trace {
		builder := strings.Builder{}
		builder.WriteString(obj.Identity())
		builder.WriteString(" Try To lock ")
		if len(print_args) >= 1 {
			builder.WriteString(fmt.Sprintf(print_args[0].(string), print_args[1:]...))
			builder.WriteRune('\n')
		}
		builder.WriteString(CreateStackTrace(1))
		log.Print(builder.String())
	}

	obj.Lock()

	if stack_trace {
		builder := strings.Builder{}
		builder.WriteString(obj.Identity())
		builder.WriteString(" Locked ")
		if len(print_args) >= 1 {
			builder.WriteString(fmt.Sprintf(print_args[0].(string), print_args[1:]...))
			builder.WriteRune('\n')
		}
		builder.WriteString(CreateStackTrace(1))
		log.Print(builder.String())
	}
}

func Unlock(obj Lockable, stack_trace bool, print_args ...interface{}) {
	if stack_trace {
		builder := strings.Builder{}
		builder.WriteString(obj.Identity())
		builder.WriteString(" Unlocked ")
		if len(print_args) >= 1 {
			builder.WriteString(fmt.Sprintf(print_args[0].(string), print_args[1:]...))
			builder.WriteRune('\n')
		}
		log.Print(builder.String())
	}
	obj.Unlock()
}

func CondWait(obj Lockable, cond *sync.Cond) {
	DPrintf("[%s] cond wait", obj.Identity())
	cond.Wait()
	DPrintf("[%s] cond start", obj.Identity())
}

func CreateStackTrace(frame_skip int) string {
	frames_ptrs := make([]uintptr, 15)
	num_frames := runtime.Callers(2+frame_skip, frames_ptrs)
	frames := runtime.CallersFrames(frames_ptrs[:num_frames])
	builder := strings.Builder{}
	if max_stack_trace < num_frames {
		num_frames = max_stack_trace
	}

	for i := 0; i < num_frames; i++ {
		frame, _ := frames.Next()
		builder.WriteString(fmt.Sprintf("%s:%d %s\n", frame.File, frame.Line, frame.Function))
	}
	return builder.String()
}

func RegisterStackTraceLimit(limit int) {
	max_stack_trace = limit
}
