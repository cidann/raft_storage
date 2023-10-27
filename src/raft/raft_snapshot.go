package raft

type InstallSnapshotArgs struct {
	Term              int    //leader’s term
	LeaderId          int    //so follower can redirect clients
	LastIncludedIndex int    //the Snapshot replaces all entries up throughand including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Data              []byte //raw bytes of the Snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int
}

type Snapshot struct {
	Data []byte
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkValidInstallSnapshotReply(args, reply) {
		return false
	}
	rf.handleValidInstallSnapshotReply(server, args, reply)

	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Lock(rf, lock_trace)
	defer Unlock(rf, lock_trace)

	if !rf.checkInstallSnapshotRequest(args, reply) {
		return
	}
	rf.handleValidInstallSnapshotRequest(args, reply)
}

func (rf *Raft) checkValidInstallSnapshotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if reply.Term == -1 {
		return false
	}
	return args.Term == rf.getTerm()
}

func (rf *Raft) handleValidInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.getTerm() {
		rf.setTerm(reply.Term)
		rf.toFollower()
		return
	}
}

func (rf *Raft) checkInstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if args.Term < rf.getTerm() {
		reply.Term = rf.getTerm()
		return false
	}
	if args.Term > rf.getTerm() {
		rf.setTerm(args.Term)
		rf.toFollower()
	}
	return true
}

func (rf *Raft) handleValidInstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.LastIncludedIndex > rf.log.first().Index() {
		if args.LastIncludedIndex > rf.log.last().Index() {
			rf.reInitializeRaftLog(args.LastIncludedIndex, args.LastIncludedTerm)
		}
		rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex)
		rf.lastApplied = Max(args.LastIncludedIndex, rf.lastApplied)
		rf.PersistSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)
		snapshot := Snapshot{
			Data: args.Data,
		}
		entry := ApplyMsg{
			CommandValid: true,
			Command:      snapshot,
			CommandIndex: args.LastIncludedIndex,
			CommandTerm:  args.LastIncludedTerm,
		}
		rf.UnlockUntilAppliable(entry)
	}
}

func (rf *Raft) makeInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log.first().Index(),
		LastIncludedTerm:  rf.log.first().Term(),
		Data:              rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) makeInstallSnapshotReply() *InstallSnapshotReply {
	return &InstallSnapshotReply{
		Term: -1,
	}
}
