package raft

import "time"

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkValidAppendReply(args, reply) {
		return false
	}
	rf.handleValidAppendReply(server, args, reply)

	return ok
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	//Remeber to finish
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkAppendRequest(args, reply) {
		return
	}
	rf.handleValidAppendRequest(args, reply)
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
		rf.UnlockAndSleepFor(GetSendTime())
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
	if reply.Success {
		replicated_up_to := getReplicatedIndex(args)
		rf.matchIndex[server] = replicated_up_to
		rf.nextIndex[server] = replicated_up_to + 1
		rf.leaderCheckAndUpdateCommit(rf.matchIndex[server])
		rf.commitCond.Broadcast()
		DPrintf("[** %d term %d] replicated on [%d] total replicated [%v] next [%v]", rf.me, rf.getTerm(), server, rf.matchIndex, rf.nextIndex)
	} else {
		//can be optimized
		rf.nextIndex[server] = rf.log.getLastTermIndex(args.PrevLogIndex) + 1
	}
}

func (rf *Raft) checkAppendRequest(args *AppendEntryArgs, reply *AppendEntryReply) bool {
	if args.Term < rf.getTerm() {
		reply.Success = false
		reply.Term = rf.getTerm()
		return false
	}
	if args.Term > rf.getTerm() {
		rf.setTerm(args.Term)
		rf.toFollower()
	}
	return true
}

func (rf *Raft) handleValidAppendRequest(args *AppendEntryArgs, reply *AppendEntryReply) {
	defer rf.lastRecord.RecordTime()
	if rf.log.checkMatch(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = true
		rf.log.replace(args.PrevLogIndex+1, args.Entries...)
		rf.toFollower()
		rf.setTerm(args.Term)
		if args.LeaderCommit < rf.log.length() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.log.length()
		}
		rf.commitCond.Broadcast()
		DPrintf("[%d term %d] Received append[len %d] from leader[%d commit %d] [index %d term %d]==[index %d term %d]", rf.me, rf.getTerm(), len(args.Entries), args.LeaderId, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, rf.log.get(args.PrevLogIndex).Index(), rf.log.get(args.PrevLogIndex).Term())
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
