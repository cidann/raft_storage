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
	log         []RaftEntry
	owner       *Raft
	start_index int
}

func (rf *Raft) initializeRaftLog(start_index, start_term int) {
	if rf.log.length() == 0 {
		rf.log.append(ApplyMsg{
			CommandIndex: start_index,
			CommandTerm:  start_term,
		})
	}
	rf.log.start_index = start_index
}

func (rf *Raft) reInitializeRaftLog(start_index, start_term int) {
	rf.log.log = []RaftEntry{
		ApplyMsg{
			CommandIndex: start_index,
			CommandTerm:  start_term,
		},
	}
	rf.log.start_index = start_index
}

func NewRaftLog(owner *Raft, entries ...RaftEntry) *RaftLog {
	return &RaftLog{
		log:   append([]RaftEntry{}, entries...),
		owner: owner,
	}
}

func (rl *RaftLog) getLogIndex(index int) int {
	return index - rl.start_index
}

func (rl *RaftLog) append(msg ...RaftEntry) {
	rl.log = append(rl.log, msg...)
	rl.owner.persist()
}

func (rl *RaftLog) replace(start int, msg ...RaftEntry) {
	start = rl.getLogIndex(start)
	if start < 0 {
		msg = msg[-start:]
		start = 0
	}
	for i := range msg {
		if i+start+rl.start_index < rl.length() {
			if rl.log[i+start] != msg[i] {
				rl.log = append(rl.log[:i+start], msg[i:]...)
				rl.owner.persist()
				break
			}
		} else {
			rl.log = append(rl.log[:i+start], msg[i:]...)
			rl.owner.persist()
			break
		}
	}
}

func (rl *RaftLog) discardUpTo(new_first_index int) {
	rl.log = rl.log[rl.getLogIndex(new_first_index):]
	rl.start_index = new_first_index
}

func (rl *RaftLog) get(index int) RaftEntry {
	if index < 0 {
		panic(fmt.Sprintf("log access with negative index %d len %d start index %d", index, rl.length(), rl.start_index))
	}
	if index >= rl.length() {
		panic(fmt.Sprintf("log access with out of bound %d len %d start index %d", index, rl.length(), rl.start_index))
	}
	if rl.getLogIndex(index) < 0 {
		panic(fmt.Sprintf("log access with negative true index %d global index %d start_index %d", rl.getLogIndex(index), index, rl.start_index))
	}
	if rl.getLogIndex(index) >= len(rl.log) {
		panic(fmt.Sprintf("log access with out of bound true index %d global index %d start_index %d", rl.getLogIndex(index), index, rl.start_index))
	}
	return rl.log[rl.getLogIndex(index)]
}

func (rl *RaftLog) getAtOrFirst(index int) RaftEntry {
	if index < 0 {
		panic(fmt.Sprintf("log access with negative index %d len %d", index, rl.length()))
	}
	if index >= rl.length() {
		panic(fmt.Sprintf("log access with out of bound %d len %d", index, rl.length()))
	}
	if rl.getLogIndex(index) >= len(rl.log) {
		panic(fmt.Sprintf("log access with out of bound true index %d global index %d start_index %d", rl.getLogIndex(index), index, rl.start_index))
	}
	if rl.getLogIndex(index) < 0 {
		return rl.first()
	}
	return rl.log[rl.getLogIndex(index)]
}

func (rl *RaftLog) getLastTermIndex(index int) int {
	i := index
	for i >= rl.start_index && rl.get(i).Term() == rl.get(index).Term() {
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
	return len(rl.log) + rl.start_index
}

func (rl *RaftLog) last() RaftEntry {
	return rl.get(rl.length() - 1)
}

func (rl *RaftLog) first() RaftEntry {
	return rl.get(rl.start_index)
}

func (rl *RaftLog) slice(begin, end int) []RaftEntry {
	if begin < 0 {
		begin = 0
	}
	if end > rl.length() {
		end = rl.length()
	}

	res := rl.log[rl.getLogIndex(begin):rl.getLogIndex(end)]

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
		if other.Term() > msg.Term() {
			return true
		} else if other.Term() == msg.Term() && other.Index() >= msg.Index() {
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
	matched := false
	if index < 0 {
		panic(fmt.Sprintf("out of bound true index %d start index %d", index, rl.start_index))
	} else if index >= rl.length() {
		matched = false
	} else if index < rl.start_index {
		matched = true
	} else {
		other = ApplyMsg{
			CommandIndex: index,
			CommandTerm:  term,
		}
		matched = matchRaftEntry(rl.get(index), other)
	}
	return matched
}

func matchRaftEntry(first, second RaftEntry) bool {
	if reflect.TypeOf(first) != reflect.TypeOf(second) {
		return false
	}
	switch first := first.(type) {
	case ApplyMsg:
		second := second.(ApplyMsg)
		return first.Term() == second.Term() && first.Index() == second.Index()
	default:
		panic("should not have other RaftEntry types")

	}

}
