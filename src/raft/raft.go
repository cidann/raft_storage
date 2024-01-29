package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"dsys/labgob"
	"dsys/labrpc"
	"fmt"
	"sync"
	"sync/atomic"
)

// import "bytes"
// import "../labgob"

type RaftState int

const (
	KILLED RaftState = iota
	FOLLOWER
	CANDIDATE
	LEADER
)

func (rf_state RaftState) String() string {
	s := ""
	switch rf_state {
	case KILLED:
		s = "KILLED"
	case FOLLOWER:
		s = "FOLLOWER"
	case CANDIDATE:
		s = "CANDIDATE"
	case LEADER:
		s = "LEADER"
	}

	return s
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leader int

	//Persistent state(persist through server failure)
	currentTerm int      //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int      //candidateId that received vote in current term (or null if none)
	log         *RaftLog //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state all server(reinitialized after startup)
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state leader(reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	voteCount  int

	lastRecord *RaftTimer
	state      RaftState
	stateCond  *sync.Cond
	commitCond *sync.Cond

	applyCh chan ApplyMsg
	mute    bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	Lock(rf, lock_trace, "GetState")
	defer Unlock(rf, lock_trace, "GetState")
	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.state == LEADER
	DPrintf("[%d] state %v", rf.me, rf.state)

	return term, isleader
}

func (rf *Raft) GetStateAndLeader() (int, int, bool) {
	Lock(rf, lock_trace, "GetStateAndLeader")
	defer Unlock(rf, lock_trace, "GetStateAndLeader")
	var term int
	var leader int
	var isleader bool

	term = rf.currentTerm
	leader = rf.leader
	isleader = rf.state == LEADER
	DPrintf("[%d] state %v leader %d", rf.me, rf.state, rf.leader)

	return term, leader, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) getRaftPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.log)
	e.Encode(rf.log.first().Index())
	e.Encode(rf.log.first().Term())
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftPersistState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) (int, int) {
	snapshot_index, snapshot_term := 0, 0
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return snapshot_index, snapshot_term
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log.log)
	d.Decode(&snapshot_index)
	d.Decode(&snapshot_term)

	return snapshot_index, snapshot_term
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	Lock(rf, lock_trace, "Start")
	defer Unlock(rf, lock_trace, "Start")
	index := -1
	term := -1
	isLeader := false

	if command == nil {
		panic("Should not have nil command")
	}
	if rf.state == LEADER {
		index = rf.log.length()
		term = rf.currentTerm
		isLeader = true
		entry := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
			CommandTerm:  term,
		}
		rf.log.append(entry)
		rf.sendAppendToFollowers()
		Debug(rf, dLeader, "S%d Got new entry {%d:%d}", rf.me, rf.log.start_index, rf.log.length())
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	Lock(rf, lock_trace, "Kill")
	defer Unlock(rf, lock_trace, "Kill")
	Debug(rf, dDrop, "S%d kill raft", rf.me)
	rf.stateCond.Broadcast()
	rf.commitCond.Broadcast()
	rf.state = KILLED
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) GetStateSize() int {
	Lock(rf, lock_trace, "GetStateSize")
	defer Unlock(rf, lock_trace, "GetStateSize")
	return rf.persister.RaftStateSize()
}

func (rf *Raft) ApplicationSnapshot(snapshot []byte, last_index, last_term int) {
	Lock(rf, lock_trace, "ApplicationSnapshot")
	defer Unlock(rf, lock_trace, "ApplicationSnapshot")
	if last_index > rf.log.first().Index() {
		rf.Snapshot(snapshot, last_index, last_term)
	}
}

func (rf *Raft) Snapshot(snapshot []byte, last_index, last_term int) {
	Debug(rf, dSnap, "S%d handle snapshot from kvserver index/term: %d/%d", rf.me, last_index, last_term)
	if last_index > rf.log.last().Index() {
		rf.reInitializeRaftLog(last_index, last_term)
	}
	rf.log.discardUpTo(last_index)
	for i := range rf.nextIndex {
		rf.setNextIndex(i, last_index+1)
	}
	if rf.commitIndex < last_index {
		rf.commitIndex = last_index
	}
	if rf.lastApplied < last_index {
		rf.lastApplied = last_index
	}

	rf.persister.SaveStateAndSnapshot(rf.getRaftPersistState(), snapshot)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = CANDIDATE
	rf.lastRecord = &RaftTimer{}
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.mute = false

	rf.log = NewRaftLog(rf)
	snapshot_index, snapshot_term := rf.readPersist(persister.ReadRaftState())

	rf.initializeStructEncode()

	rf.initializeRaftLog(snapshot_index, snapshot_term)
	rf.commitIndex = snapshot_index
	rf.lastApplied = snapshot_index
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.startWaitForResponse()
	go rf.commitDaemon()

	DPrintf("[%d] Start goroutine to start the election process", rf.me)

	return rf
}

func (rf *Raft) initializeStructEncode() {
	labgob.Register(ApplyMsg{})
}

func (rf *Raft) getTerm() int {
	return rf.currentTerm
}

func (rf *Raft) getVotedFor() int {
	return rf.votedFor
}

func (rf *Raft) setTerm(term int) {
	if term != rf.currentTerm {
		rf.currentTerm = term
		rf.persist()
	}
}

func (rf *Raft) setVotedFor(vote int) {
	if vote != rf.votedFor {
		rf.votedFor = vote
		rf.persist()
	}
}

func (rf *Raft) setTermAndVote(term, vote int) {
	if term != rf.currentTerm && vote != rf.votedFor {
		rf.currentTerm = term
		rf.votedFor = vote
		rf.persist()
	}
}

func (rf *Raft) setNextIndex(server, index int) {
	nxt_index := index
	if index <= rf.log.start_index {
		nxt_index = rf.log.start_index + 1
	}
	rf.nextIndex[server] = nxt_index
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) Identity() string {
	return fmt.Sprintf("[%d]", rf.me)
}
