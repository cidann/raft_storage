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
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)
	go func() {
		Lock(rf, lock_trace)
		defer Unlock(rf, lock_trace)
		rf.stateCond.Broadcast()
	}()
	for !rf.killed() {
		//Halt election clock if state is not candidate or followerer
		for rf.state == LEADER && !rf.killed() {
			rf.stateCond.Wait()
		}
		election_time := GetElectionTime()
		if rf.lastRecord.HasElapsed(election_time) {
			DPrintf("[%d term %d] Goroutine awake and election timed out", rf.me, rf.getTerm())
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
	rf.setTerm(rf.getTerm() + 1)
	rf.voteCount = 1
	rf.setVotedFor(rf.me)
	rf.state = CANDIDATE

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.setNextIndex(i, -1)
			rf.matchIndex[i] = -1
			continue
		}
		rf.setNextIndex(i, rf.log.length())
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
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkVoteRequest(args, reply) {
		return
	}
	rf.handleValidVoteRequest(args, reply)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DelaySchedule(RPCVoteDelay)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkValidVoteReply(args, reply) {
		return false
	}
	rf.handleValidVoteReply(args, reply)
	DPrintf("[%d term %d] Goroutine to collect vote. Current count: %d", rf.me, rf.getTerm(), rf.voteCount)

	return ok
}

func (rf *Raft) makeRequestVoteArg() *RequestVoteArgs {
	lastEntry := rf.log.last()
	return &RequestVoteArgs{
		Term:           rf.getTerm(), //candidate’s term
		CandidateId:    rf.me,        //candidate requesting vote
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
	return args.Term == rf.getTerm()
}

func (rf *Raft) handleValidVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	if reply.Term > rf.getTerm() {
		rf.setTerm(reply.Term)
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
	if args.Term < rf.getTerm() {
		reply.VoteGranted = false
		reply.Term = rf.getTerm()
		return false
	}
	if args.Term > rf.getTerm() {
		rf.setTerm(args.Term)
		rf.toFollower()
	}
	return true
}

func (rf *Raft) handleValidVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	if (rf.getVotedFor() == -1) && rf.log.isUpToDate(args.LastEntryIndex, args.LastEntryTerm) {
		reply.VoteGranted = true
		rf.lastRecord.RecordTime()
		Debug(rf, dVote, "S%d voted for %d for term %d prev voted %d", rf.me, args.CandidateId, args.Term, rf.getVotedFor())
		rf.setVotedFor(args.CandidateId)
		DPrintf("[%d term %d] Received vote from more up to date server [index %d term %d]>[index %d term %d]", rf.me, rf.getTerm(), args.LastEntryIndex, args.LastEntryTerm, rf.log.last().Index(), rf.log.last().Term())
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.getTerm()
}

func (rf *Raft) toFollower() {
	//Debug(rf, dLeader, "S%d converted to follower from %s trace: %s", rf.me, rf.state, CreateStackTrace(1))
	rf.setVotedFor(-1)
	if rf.state == FOLLOWER {
		return
	}

	rf.state = FOLLOWER
	rf.stateCond.Broadcast()
}
