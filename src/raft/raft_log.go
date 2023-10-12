package raft

import (
	"fmt"
	"reflect"
)

type RaftEntry interface {
	/*
		true if other RaftEntry is as up to date as called RaftEntry
	*/
	OtherUpToDate(other RaftEntry) bool
	Index() int
	Term() int
}

type RaftLog struct {
	log []RaftEntry
}

func (rf *Raft) intializeRaftLog() {
	pad := ApplyMsg{
		CommandIndex: 0,
		CommandTerm:  0,
	}
	rf.log = NewRaftLog(pad)
}

func NewRaftLog(entries ...RaftEntry) *RaftLog {
	return &RaftLog{
		log: append([]RaftEntry{}, entries...),
	}
}

func (rl *RaftLog) append(msg ...RaftEntry) {
	rl.log = append(rl.log, msg...)
}

func (rl *RaftLog) replace(start int, msg ...RaftEntry) {
	rl.log = append(rl.log[:start], msg...)
}

func (rl *RaftLog) get(index int) RaftEntry {
	if index < 0 {
		panic(fmt.Sprintf("log access with negative index %d len %d", index, rl.length()))
	}
	if index >= len(rl.log) {
		panic(fmt.Sprintf("log access with out of bound %d len %d", index, rl.length()))
	}
	return rl.log[index]
}
func (rl *RaftLog) getLastTermIndex(index int) int {
	term := rl.get(index).Term()
	i := index
	for rl.get(i).Term() == term {
		i--
	}
	return i
}

func (rl *RaftLog) getTermFirstIndex(index int) int {
	term := rl.get(index).Term()
	i := index
	for rl.get(i).Term() == term {
		i--
	}
	return i + 1
}

func (rl *RaftLog) length() int {
	return len(rl.log)
}

func (rl *RaftLog) last() RaftEntry {
	return rl.get(len(rl.log) - 1)
}

func (rl *RaftLog) slice(begin, end int) []RaftEntry {
	if begin < 0 {
		begin = 0
	}
	if end > rl.length() {
		end = rl.length()
	}

	res := rl.log[begin:end]

	return res
}

func (rl *RaftLog) isUpToDate(index, term int) bool {
	other := ApplyMsg{
		CommandIndex: index,
		CommandTerm:  term,
	}
	return rl.last().OtherUpToDate(other)
}

func (msg ApplyMsg) OtherUpToDate(other RaftEntry) bool {
	switch other := other.(type) {
	case ApplyMsg:
		if other.CommandTerm > msg.CommandTerm {
			return true
		} else if other.CommandTerm == msg.CommandTerm && other.CommandIndex >= msg.CommandIndex {
			return true
		}
		return false
	default:
		panic("should not have other RaftEntry types")
	}
}

func (msg ApplyMsg) Index() int {
	return msg.CommandIndex
}

func (msg ApplyMsg) Term() int {
	return msg.CommandTerm
}

func (rl *RaftLog) checkMatch(index, term int) bool {
	var other RaftEntry
	if index < 0 {
		panic("Don't allow left bound for now")
	} else if index >= rl.length() {
		return false
	} else {
		other = ApplyMsg{
			CommandIndex: index,
			CommandTerm:  term,
		}
	}
	return matchRaftEntry(rl.get(other.Index()), other)
}

func matchRaftEntry(first, second RaftEntry) bool {
	if reflect.TypeOf(first) != reflect.TypeOf(second) {
		return false
	}
	switch first := first.(type) {
	case ApplyMsg:
		second := second.(ApplyMsg)
		return first.CommandTerm == second.CommandTerm && first.CommandIndex == second.CommandIndex
	default:
		panic("should not have other RaftEntry types")

	}

}
