package raft

import (
	"math/rand"
	"time"
)

/*
Raft is locked and unlocked at:
startWaitForResponse
sendRequestVote RPC response
*/

func GetElectionTime() time.Duration {
	r := rand.Intn(500) + 300
	return time.Duration(r) * time.Millisecond
}
func GetMaxElectionTime() time.Duration {
	return time.Duration(800) * time.Millisecond
}

func (rf *Raft) startWaitForResponse() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.stateCond.Broadcast()
	}()
	for !rf.killed() {
		//Halt election clock if state is not candidate or followerer
		for rf.state == LEADER {
			rf.stateCond.Wait()
		}
		election_time := GetElectionTime()
		if rf.lastRecord.HasElapsed(election_time) {
			DPrintf("[%d term %d] Goroutine awake and election timed out", rf.me, rf.currentTerm)
			rf.startElection()
		}
		rf.UnlockAndSleepFor(election_time)
	}
}

func (rf *Raft) startElection() {
	rf.initCandidateState()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, rf.makeRequestVoteArg(), rf.makeRequestVoteReply())
	}
}

func (rf *Raft) initCandidateState() {
	rf.currentTerm += 1
	rf.voteCount = 1
	rf.votedFor = rf.me
	rf.state = CANDIDATE

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.nextIndex[i] = -1
			rf.matchIndex[i] = -1
			continue
		}
		rf.nextIndex[i] = rf.log.length()
		rf.matchIndex[i] = 0
	}
	rf.lastRecord.RecordTime()
}

type RequestVoteArgs struct {
	Term           int //candidate’s term
	CandidateId    int //candidate requesting vote
	LastEntryIndex int
	LastEntryTerm  int
}

type RequestVoteReply struct {
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkVoteRequest(args, reply) {
		return
	}
	rf.handleValidVoteRequest(args, reply)
}

// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DelaySchedule(RPCVoteDelay)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkValidVoteReply(args, reply) {
		return false
	}
	rf.handleValidVoteReply(args, reply)
	DPrintf("[%d term %d] Goroutine to collect vote. Current count: %d", rf.me, rf.currentTerm, rf.voteCount)

	return ok
}

func (rf *Raft) makeRequestVoteArg() *RequestVoteArgs {
	lastEntry := rf.log.last()
	return &RequestVoteArgs{
		Term:           rf.currentTerm, //candidate’s term
		CandidateId:    rf.me,          //candidate requesting vote
		LastEntryIndex: lastEntry.Index(),
		LastEntryTerm:  lastEntry.Term(),
	}
}

func (rf *Raft) makeRequestVoteReply() *RequestVoteReply {
	return &RequestVoteReply{
		Term:        -1,    //candidate’s term
		VoteGranted: false, //candidate requesting vote
	}
}

func (rf *Raft) checkValidVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if reply.Term == -1 {
		return false
	}
	return args.Term == rf.currentTerm
}

func (rf *Raft) handleValidVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.toFollower()
		return
	}
	if reply.VoteGranted {
		rf.voteCount += 1
	}
	if rf.voteCount > len(rf.peers)/2 {
		go rf.startLeader()
	}
}

func (rf *Raft) checkVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return false
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
	}
	return true
}

func (rf *Raft) handleValidVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.log.isUpToDate(args.LastEntryIndex, args.LastEntryTerm) {
		reply.VoteGranted = true
		rf.lastRecord.RecordTime()
		rf.votedFor = args.CandidateId
		DPrintf("[%d term %d] Received vote from more up to date server [index %d term %d]>[index %d term %d]", rf.me, rf.currentTerm, args.LastEntryIndex, args.LastEntryTerm, rf.log.last().Index(), rf.log.last().Term())
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) toFollower() {
	DPrintf("[%d term %d] converted to follower", rf.me, rf.currentTerm)
	rf.votedFor = -1
	if rf.state == FOLLOWER {
		return
	}
	rf.state = FOLLOWER
	rf.stateCond.Broadcast()
}
