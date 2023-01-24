package raft


import "sync"
import "sync/atomic"
import "../labrpc"
import (
	"time"
	"math/rand"
	"runtime"
	"fmt"
)

import "bytes"
import "../labgob"

func HoldFMT(){fmt.Println()}

//Command wrapper
const READ,WRITE="r","w"
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm int
	Type string
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
	currentTerm int //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int //candidateId that received vote in current term (or null if none)
	logState *LogState

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	//(Reinitialized after election)
	nextIndex []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	
	state int
	leader int
	lastAppend time.Time

	applicationChan chan ApplyMsg
	committing bool

}


////////////////////////////////////////////////////////////////////////////////////////////////
//region Setters

/*
param:currentTerm
setter for currentTerm and write to persister
*/

func (rf *Raft) SetCurrentTerm(currentTerm int){
	rf.currentTerm=currentTerm
	rf.persist()
}

/*
param:votedFor
setter for votedFor and write to persister
*/

func (rf *Raft) SetVoteFor(votedFor int){
	rf.votedFor=votedFor
	rf.persist()
}

/*
param:log
setter for log entry and write to persister
*/

func (rf *Raft) SetLog(log []*ApplyMsg, newIndex int){
	writeExist:=false
	for i:=newIndex;i<len(log);i++{
		if log[i].Type==WRITE{
			writeExist=true
			break
		}
	}
	rf.logState.SetLog(log)
	if writeExist{
		rf.persist()
	}
}

/*
param:currentTerm,votedFor
setter for both currentTerm and votedFor at same time to reduce write to persist 
*/

func (rf *Raft) SetTermAndVote(currentTerm int,votedFor int){
	rf.currentTerm=currentTerm
	rf.votedFor=votedFor
	rf.persist()
}


func (rf *Raft) SetSnapshot(latestIndex,latestTerm int,snapBytes []byte){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logState.SetSnapshot(latestIndex,latestTerm,snapBytes)
	rf.persist()
}

/*
param:state
setter for state and start waiting clock if previous state was 1
*/

func (rf *Raft) SetState(state int){
	DPrintf("SetState from %d to %d\n",rf.state,state)
	if rf.state==1&&state!=1{
		go rf.WaitForResponse()
		rf.lastAppend=time.Now()
	}
	if state==1{
		rf.leader=rf.me
	}
	rf.state=state
}


//endregion


////////////////////////////////////////////////////////////////////////////////////////////////
//region Getters 

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term=rf.currentTerm
	isleader=rf.state==1
	rf.mu.Unlock()
	return term,isleader
}


func GetSendTime()time.Duration{
	return time.Duration(110)*time.Millisecond
}

func GetElectionTime()time.Duration{
	r := rand.Intn(500)+300
	return time.Duration(r)*time.Millisecond
}

func GetMaxEletionTime()time.Duration{
	r := 800
	return time.Duration(r)*time.Millisecond
}

func (rf *Raft)GetLeader(lock bool)int{
	if lock{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.leader
	} else{
		return rf.leader
	}
}

func (rf *Raft)GetPersister()*Persister{
	return rf.persister
}

func (rf *Raft)GetLogState()*LogState{
	return rf.logState
}

//endregion

////////////////////////////////////////////////////////////////////////////////////////////////
//region Helper function


/*
change the state of raft to leader and startLeader timer
*/

func (rf *Raft)changeToLeader(){
	rf.SetState(1)
	for i:=range rf.nextIndex{
		rf.nextIndex[i]=rf.logState.Len()
		rf.matchIndex[i]=-1
	}
	go rf.StartLeader()
}




/*
param: server
Move NextIndex of server to first found index with previous term
*/

func (rf *Raft)NextToPrevTerm(server int){
	prevIndex:=rf.nextIndex[server]
	if prevIndex>=rf.logState.Len(){
		prevIndex=rf.logState.Len()-1
	}
	curTerm:=rf.logState.GetEntry(prevIndex).CommandTerm
	newIndex:=prevIndex
	for ;newIndex>0&&rf.logState.GetEntry(newIndex).CommandTerm==curTerm;newIndex--{}
	rf.nextIndex[server]=newIndex
}

/*
param:otherIndex,otherTerm
compare raft's latest log with otherIndex and otherTerm to see if otherIndex and otherTerm are up to date
*/

func (rf *Raft) upToDate(otherIndex,otherTerm int)bool{
	term:=rf.logState.GetEntry(rf.logState.Len()-1).CommandTerm
	if term>otherTerm{
		return false
	} else if term<otherTerm{
		return true
	} else{
		return otherIndex>=rf.logState.Len()-1
	}
}

/*
Generate args and reply for append request rpcs based on current raft state
*/

func (rf *Raft) AppendArgReply() ([]*AppendEntriesArgs,[]*AppendEntriesReply){
	args:=make([]*AppendEntriesArgs,len(rf.peers))
	replies:=make([]*AppendEntriesReply,len(rf.peers))
	for i:=range rf.peers{
		args[i]=&AppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId:rf.me,
			PrevLogIndex:rf.nextIndex[i]-1,
			PrevLogTerm:rf.logState.GetEntry(rf.nextIndex[i]-1).CommandTerm,
			Entries:DeepCopyLog(rf.logState.SliceLog(rf.nextIndex[i],rf.logState.Len())),
			LeaderCommit:rf.commitIndex,
		}
		replies[i]=&AppendEntriesReply{}
	}
	return args,replies
}

/*
Generate args and reply for vote request rpcs based on current raft state
*/

func (rf *Raft) VoteArgReply() ([]*RequestVoteArgs,[]*RequestVoteReply){
	args:=make([]*RequestVoteArgs,len(rf.peers))
	replies:=make([]*RequestVoteReply,len(rf.peers))
	for i:=range rf.peers{
		args[i]=&RequestVoteArgs{
			Term:rf.currentTerm,
			CandidateId:rf.me,
			LastLogIndex:rf.logState.Len()-1,
			LastLogTerm:rf.logState.GetEntry(rf.logState.Len()-1).CommandTerm,
		}
		replies[i]=&RequestVoteReply{}
	}
	return args,replies
}

//endregion



////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////
//region RPC and state machine actions


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int//candidate’s term
	CandidateId int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int//currentTerm, for candidate to update itself
	VoteGranted bool//true means candidate received vote
}


type AppendEntriesArgs struct{
	Term int//leader’s term
	LeaderId int//so follower can redirect clients
	PrevLogIndex int//index of log entry immediately preceding new ones
	PrevLogTerm int //term of prevLogIndex entry
	Entries []*ApplyMsg //entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int//leader’s commitIndex
}

type AppendEntriesReply struct{
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

type InstallSnapshotArgs struct{
	Term int //leader’s term
	LeaderId int //so follower can redirect clients
	LastIncludedIndex int //the snapshot replaces all entries up through and including this index
	LastIncludedTerm int //term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
	Data []byte //raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct{
	Term int //for leader update
}

//
//RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	DPrintf("[%d]Received vote request from %d \n",rf.me,args.CandidateId)
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term>rf.currentTerm{
		rf.SetState(3)
		rf.SetTermAndVote(args.Term,-1)
		reply.Term=args.Term
	} else{
		reply.Term=rf.currentTerm
	}
	if args.Term>=rf.currentTerm&&(rf.votedFor==-1||rf.votedFor==args.CandidateId)&&rf.upToDate(args.LastLogIndex,args.LastLogTerm){
		reply.VoteGranted=true
		rf.SetVoteFor(args.CandidateId)
		rf.lastAppend=time.Now()
		DPrintf("[%d]Vote granted to %d \n",rf.me,args.CandidateId)
	} else{
		reply.VoteGranted=false
		DPrintf("[%d]Vote not granted to %d because %t %t %t\n",rf.me,args.CandidateId,args.Term>=rf.currentTerm,rf.votedFor==-1||rf.votedFor==args.CandidateId,rf.upToDate(args.LastLogIndex,args.LastLogTerm))
	}
}


//
//RequestAppend RPC handler.
//
func (rf *Raft) RequestAppend(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] Received append request from %d\n",rf.me,args.LeaderId)
	if args.Term>rf.currentTerm{
		rf.SetState(3)
		rf.SetTermAndVote(args.Term,-1)
		reply.Term=args.Term
	} else if args.Term<rf.currentTerm{
		reply.Term=rf.currentTerm
		reply.Success=false
		return
	} else{
		reply.Term=args.Term
	}

	if rf.logState.GetEntry(args.PrevLogIndex).CommandTerm==args.PrevLogTerm{
		i:=0
		for ;i+args.PrevLogIndex+1<rf.logState.Len()&&i<len(args.Entries);i++{
			if rf.logState.GetEntry(i+args.PrevLogIndex+1).CommandTerm!=args.Entries[i].CommandTerm{
				break
			}
		}
		if i<len(args.Entries)&&args.PrevLogIndex+1>rf.commitIndex{
			
			rf.SetLog(append(rf.logState.SliceLog(0,i+args.PrevLogIndex+1),args.Entries[i:]...),i+args.PrevLogIndex+1)
		}

		if rf.commitIndex<args.LeaderCommit{
			rf.commitIndex=args.LeaderCommit
		}
		
		if !rf.committing{
			go rf.commit()
		}
		
		rf.SetState(3)
		reply.Success=true
		rf.leader=args.LeaderId

	} else{
		reply.Success=false
	}
	
	rf.lastAppend=time.Now()

}

//
//RequestInstallSnapShot RPC handler.
//
func (rf *Raft) RequestInstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	DPrintf("[%d]Received install snapshot request from %d \n",rf.me,args.LeaderId)
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term>rf.currentTerm{
		rf.SetState(3)
		rf.SetTermAndVote(args.Term,-1)
		reply.Term=args.Term
	} else if args.Term<rf.currentTerm{
		reply.Term=rf.currentTerm
		return
	} else{
		reply.Term=args.Term
	}

	if rf.logState.GetSnapshot().LastIndex<args.LastIncludedIndex{
		
	}
}



/*
Request vote from server
*/
func (rf *Raft) sendRequestVote(server int, arg *RequestVoteArgs, reply *RequestVoteReply,voteStatus []bool) {
	ok:=false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed()&&rf.state==2&&rf.currentTerm==arg.Term&&!ok{
		rf.mu.Unlock()
		ok=rf.peers[server].Call("Raft.RequestVote", arg, reply)
		rf.mu.Lock()

		//check reply term and up to date this check for reply up to date
		if reply.Term>rf.currentTerm{
			rf.SetTermAndVote(reply.Term,-1)
			rf.SetState(3)
			rf.lastAppend=time.Now()
			return
		}

		if ok&&!rf.killed()&&arg.Term==rf.currentTerm&&rf.state==2&&reply.VoteGranted{
			voteStatus[server]=true
			count:=0
			for _,v:=range voteStatus{
				if v{
					count+=1
				}
			}
			if count>len(rf.peers)/2{
				rf.changeToLeader()
			}
		}
	}
}

/*
Append log entries to server
*/
func (rf *Raft) sendAppendEntries(server int, arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed()||arg.Term!=rf.currentTerm||rf.state!=1{
		return
	}


	DPrintf("[%d] Send append request %d\n",rf.me,server)
	//the request

	rf.mu.Unlock()
	ok:=rf.peers[server].Call("Raft.RequestAppend", arg, reply)
	rf.mu.Lock()
	
	if rf.killed()||rf.state!=1{
		return
	}

	if reply.Term>rf.currentTerm{
		rf.SetTermAndVote(reply.Term,-1)
		rf.SetState(3)
		rf.lastAppend=time.Now()
		return
	}

	if ok&&!reply.Success&&rf.nextIndex[server]>0{
		rf.NextToPrevTerm(server)
	} else if ok&&!rf.killed()&&arg.Term==rf.currentTerm&&arg.PrevLogIndex+len(arg.Entries)>rf.matchIndex[server]{
		rf.matchIndex[server]=arg.PrevLogIndex+len(arg.Entries)
		rf.nextIndex[server]=rf.matchIndex[server]+1
		if rf.logState.GetEntry(rf.matchIndex[server]).CommandTerm==rf.currentTerm{
			count:=1

			for _,appliedIndex:=range rf.matchIndex{
				if appliedIndex>=rf.matchIndex[server]{
					count+=1
				}
			}

			if count>len(rf.peers)/2&&rf.commitIndex<rf.matchIndex[server]{
				rf.commitIndex=rf.matchIndex[server]
				if !rf.committing{
					go rf.commit()
				}
			}
		}
	}
}

/*
Request vote from server
*/
func (rf *Raft) sendInstallSnapshot(server int, arg *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.mu.Unlock()
	ok:=rf.peers[server].Call("Raft.RequestInstallSnapshot", arg, reply)
	rf.mu.Lock()
		
	if !ok||rf.killed()||rf.state!=1{
		return
	}

	if reply.Term>rf.currentTerm{
		rf.SetTermAndVote(reply.Term,-1)
		rf.SetState(3)
		rf.lastAppend=time.Now()
		return
	}

	
}



/*
Act as timer that create goroutine for all each other servers when heartbeat time passed
*/

func (rf *Raft)StartLeader(){
	DPrintf("[%d] became leader for term (%d)\n",rf.me,rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.state==1&&!rf.killed(){
		DPrintf("%v next index\n",rf.nextIndex)
		
		args,replies:=rf.AppendArgReply()

		for i:=range rf.peers{
			if i!=rf.me{
				go rf.sendAppendEntries(i,args[i],replies[i])
			}
		}
		rf.mu.Unlock()
		time.Sleep(GetSendTime())
		rf.mu.Lock()
	}
}

/*
Create goroutine for each other server which repeat until rpc success or invalid
*/

func (rf *Raft)StartElection(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed()||rf.state!=2{
		return
	}
	rf.currentTerm+=1
	DPrintf("[%d] starts election with term %d\n",rf.me,rf.currentTerm)
	voteStatus:=make([]bool,len(rf.peers))
	voteStatus[rf.me]=true

	args,replies:=rf.VoteArgReply()
	
	for i:=range rf.peers{
		if i!=rf.me{
			go rf.sendRequestVote(i,args[i],replies[i],voteStatus)
		}
	}
}

/*
Act as timer for follower/candidate
Check every 80ms and start election if election time passed without message
*/
func (rf *Raft)WaitForResponse(){
	electionTime:=GetElectionTime()
	for true{
		rf.mu.Lock()
		if rf.state==1||rf.killed(){
			rf.mu.Unlock()
			return
		}
		if !(rf.state==1||rf.killed())&&time.Since(rf.lastAppend)>=electionTime{
			//start election
			electionTime=GetElectionTime()
			rf.SetState(2)
			rf.lastAppend=time.Now()
			go rf.StartElection()
		} 
		rf.mu.Unlock()
		time.Sleep(50*time.Millisecond)
	}
}

/*
Commit the entries from last applied to commitIndex
*/

func (rf *Raft)commit(){
	rf.mu.Lock()
	rf.committing=true
	defer func(){
		rf.committing=false
		rf.mu.Unlock()
	}()
	
	for rf.lastApplied<rf.commitIndex&&rf.lastApplied+1<rf.logState.Len(){
		rf.lastApplied++
		DPrintf("[%d] Commit entry %d\n",rf.me,rf.lastApplied)
		entry:=rf.logState.GetEntry(rf.lastApplied)
		if !entry.CommandValid{
			break
		}
		rf.mu.Unlock()
		rf.applicationChan<-*entry
		rf.mu.Lock()
	}
}
/*
Append write command
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.state!=1{
		isLeader=false
		return index, term, isLeader
	}
	index=rf.logState.Len()
	term=rf.currentTerm
	newCommand:=&ApplyMsg{
		CommandValid:true,
		Command:command,
		CommandIndex:index,
		CommandTerm:term,
		Type:WRITE,
	}
	rf.SetLog(append(rf.logState.GetLog(),newCommand),rf.logState.Len())
	DPrintf("*[%d]Leader gets new entry\n",rf.me)
	return index, term, isLeader
}
/*
Append read command
*/
func (rf *Raft) TryRead(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	if rf.state!=1{
		isLeader=false
		return index, term, isLeader
	}
	index=rf.logState.Len()
	term=rf.currentTerm
	newCommand:=&ApplyMsg{
		CommandValid:true,
		Command:command,
		CommandIndex:index,
		CommandTerm:term,
		Type:READ,
	}
	rf.SetLog(append(rf.logState.GetLog(),newCommand),rf.logState.Len())
	DPrintf("*[%d]Leader gets new entry\n",rf.me)
	return index, term, isLeader
}

/*
Mimic server failure
*/
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
/*
For testing to see server failed
*/
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*
param: peers, me, persister, applych
Create raft server with peers of other raft server
me identify the index inside peers that is self
persister saves data to prevent lost during server failure
applyCh funnels commited command to application layer
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm=0
	rf.votedFor=-1
	rf.logState=&LogState{[]*ApplyMsg{},&Snapshot{LastIndex:-1,LastTerm:-1}}

	//Volatile state on all servers:
	rf.commitIndex=rf.logState.GetSnapshot().LastIndex
	rf.lastApplied=rf.commitIndex

	//Volatile state on leaders:
	//(Reinitialized after election)
	rf.nextIndex=make([]int,len(rf.peers))
	rf.matchIndex=make([]int,len(rf.peers))

	
	rf.state=3
	rf.leader=-1
	rf.lastAppend=time.Now()
	rf.applicationChan=applyCh

	go rf.WaitForResponse()
	DPrintf("================ number of goroutine alive %d================\n",runtime.NumGoroutine())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	return rf
}


/*
Save raft state to persistent storage
*/
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot:=rf.logState.GetSnapshot()

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(snapshot.LastIndex)
	e.Encode(snapshot.LastTerm)
	e.Encode(rf.logState.GetLog())

	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,snapshot.SnapBytes)
}


/*
Restore state from persistent storage
*/
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	rState := bytes.NewBuffer(data)
	dState := labgob.NewDecoder(rState)
	
	var currentTerm,votedFor,lastIndex,lastTerm int
	var log []*ApplyMsg
	snapByte:=rf.persister.ReadSnapshot()

	if dState.Decode(&currentTerm) != nil ||
	dState.Decode(&votedFor) != nil ||
	dState.Decode(&lastIndex) != nil||
	dState.Decode(&lastTerm) != nil ||
	dState.Decode(&log) != nil{
		panic("Loading Error\n")
	} else {
	  	rf.currentTerm = currentTerm
	  	rf.votedFor = votedFor
	  	rf.logState=&LogState{
			log,
			&Snapshot{
				lastIndex,
				lastTerm,
				snapByte,
			},
		}
	}
}

//endregion