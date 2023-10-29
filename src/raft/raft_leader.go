package raft

import (
	"time"
)

/*
Raft is locked and unlocked at:
startLeader
commitDaemon
RPCs
*/

type AppendEntryArgs struct {
	Term         int //leader’s term
	LeaderId     int //so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftEntry // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int         //leader’s commitIndex
}

type AppendEntryReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

}

func GetSendTime() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	DelaySchedule(RPCAppendDelay)

	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkValidAppendReply(args, reply) {
		return false
	}
	rf.handleValidAppendReply(server, args, reply)

	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//Remeber to finish
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkAppendRequest(args, reply) {
		return
	}
	rf.handleValidAppendRequest(args, reply)
}

func (rf *Raft) startLeader() {
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if rf.state == LEADER {
		return
	}

	rf.state = LEADER
	DPrintf("[** %d term %d] %v is now leader!", rf.me, rf.getTerm(), rf.nextIndex)

	for rf.state == LEADER && !rf.killed() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntry(i, rf.makeAppendEntryArgs(i), rf.makeAppendEntryReply())
		}
		DPrintf("[** %d term %d] sent a new wave of appends!", rf.me, rf.getTerm())
		UnlockAndSleepFor(rf, GetSendTime())
	}
}

func (rf *Raft) checkValidAppendReply(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if reply.Term == -1 {
		return false
	}
	return args.Term == rf.getTerm()
}

func (rf *Raft) handleValidAppendReply(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if reply.Term > rf.getTerm() {
		rf.setTerm(reply.Term)
		rf.toFollower()
		return
	}

	//There is not need to worry about redundant snapshot rpcs
	//since the follower that is processing the snapsho rpc will have the lock and request will timeout
	//If reply reach this point it means it did not timeout or outdated thus at this point
	//we can conclude the follower indeed does not have the snapshot if their next index is out of bound
	if reply.Success {
		replicated_up_to := getReplicatedIndex(args)
		rf.matchIndex[server] = replicated_up_to
		rf.setNextIndex(server, replicated_up_to+1)
		rf.leaderCheckAndUpdateCommit(rf.matchIndex[server])
		rf.commitCond.Broadcast()
		DPrintf("[** %d term %d] replicated on [%d] total replicated [%v] next [%v]", rf.me, rf.getTerm(), server, rf.matchIndex, rf.nextIndex)
	} else {
		next := rf.log.getLastTermIndex(args.PrevLogIndex) + 1
		if rf.log.getLogIndex(next) <= 0 && args.PrevLogIndex == rf.log.start_index {
			go rf.sendInstallSnapshot(server, rf.makeInstallSnapshotArgs(), rf.makeInstallSnapshotReply())
		}
		rf.setNextIndex(server, next)
	}
}

func (rf *Raft) checkAppendRequest(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	valid := true
	if args.Term < rf.getTerm() {
		reply.Success = false
		reply.Term = rf.getTerm()
		valid = false
	}
	if args.PrevLogIndex+1+len(args.Entries) <= rf.commitIndex {
		valid = false
	}
	if args.Term > rf.getTerm() {
		rf.setTerm(args.Term)
		rf.toFollower()
	}

	return valid
}

func (rf *Raft) handleValidAppendRequest(args *AppendEntryArgs, reply *AppendEntryReply) {
	defer rf.lastRecord.RecordTime()
	DPrintf("[%d term %d] Received append[len %d] from leader[%d commit %d] [index %d term %d]==[index %d term %d]", rf.me, rf.getTerm(), len(args.Entries), args.LeaderId, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, args.PrevLogIndex)

	if rf.log.checkMatch(args.PrevLogIndex, args.PrevLogTerm) {
		if rf.commitIndex < rf.log.start_index {
			panic("commit index smaller than begining index")
		}
		reply.Success = true
		rf.log.replace(args.PrevLogIndex+1, args.Entries...)
		rf.toFollower()
		rf.setTerm(args.Term)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.log.length()-1)
			rf.commitCond.Broadcast()
		}
		if rf.log.length() <= rf.commitIndex {
			panic("Some commited log was removed")
		}
	} else {
		reply.Success = false
	}

	rf.leader = args.LeaderId
	reply.Term = rf.getTerm()
}

//Only update commit index for now
func (rf *Raft) leaderCheckAndUpdateCommit(new_commit_index int) {
	count := 0
	if new_commit_index <= rf.commitIndex || rf.log.get(new_commit_index).Term() < rf.getTerm() {
		return
	}
	for i, replicatedIndex := range rf.matchIndex {
		if replicatedIndex >= new_commit_index || i == rf.me {
			count++
		}
	}
	if count > len(rf.peers)/2 {
		rf.commitIndex = new_commit_index
	}
}

func (rf *Raft) commitDaemon() {
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)
	for !rf.killed() {
		rf.commitCond.Wait()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log.get(rf.lastApplied).(ApplyMsg)
			entry.CommandValid = true
			rf.UnlockUntilAppliable(entry)
		}
	}
}

func (rf *Raft) makeAppendEntryArgs(server int) *AppendEntryArgs {
	prevEntry := rf.log.get(rf.nextIndex[server] - 1)
	return &AppendEntryArgs{
		Term:         rf.getTerm(),
		LeaderId:     rf.me,
		PrevLogIndex: prevEntry.Index(),
		PrevLogTerm:  prevEntry.Term(),
		Entries:      rf.log.slice(rf.nextIndex[server], rf.log.length()),
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) makeAppendEntryReply() *AppendEntryReply {
	return &AppendEntryReply{
		Term:    -1,
		Success: false,
	}
}

//bug prev index is -1 and Entries contain DefaultEntry making len() 1 thus
//code assumes log [0:1] is replicated and move nextindex to 1
func getReplicatedIndex(args *AppendEntryArgs) int {
	return args.PrevLogIndex + 1 + len(args.Entries) - 1
}
