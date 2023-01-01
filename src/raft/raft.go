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

import "sync"
import "sync/atomic"
import "../labrpc"
import (
	"time"
	"math/rand"
	"runtime"
)

import "bytes"
import "../labgob"



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
	CommandTerm int
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
	log [] ApplyMsg //each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:
	//(Reinitialized after election)
	nextIndex []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	
	state int
	lastAppend time.Time

	applicationChan chan ApplyMsg
	committing bool
}

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
	return term, isleader
}

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

func (rf *Raft) SetLog(log []ApplyMsg){
	rf.log=log
	rf.persist()
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

/*
param:otherIndex,otherTerm
compare raft's latest log with otherIndex and otherTerm to see if otherIndex and otherTerm are up to date
*/

func (rf *Raft) upToDate(otherIndex,otherTerm int)bool{
	term:=rf.getIndexTerm(len(rf.log)-1)
	if term>otherTerm{
		return false
	} else if term<otherTerm{
		return true
	} else{
		return otherIndex>=len(rf.log)-1
	}
}

/*
Generate heartbeat send time
*/

func getSendTime()time.Duration{
	return time.Duration(110)*time.Millisecond
}

/*
Generate randomized election time
*/

func getElectionTime()time.Duration{
	r := rand.Intn(500)+1000
	return time.Duration(r)*time.Millisecond
}

/*
safe get for Term at index
*/

func (rf *Raft)getIndexTerm(index int)int{
	if index<0||index>=len(rf.log){
		return -1
	}
	return rf.log[index].CommandTerm
}


/*
change the state of raft to leader and startLeader timer
*/

func (rf *Raft)changeToLeader(){
	rf.state=1
	for i:=range rf.nextIndex{
		rf.nextIndex[i]=len(rf.log)
		rf.matchIndex[i]=-1
	}
	go rf.StartLeader()
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
	for rf.lastApplied<rf.commitIndex&&rf.lastApplied+1<len(rf.log){
		rf.lastApplied++
		DPrintf("[%d] Commit entry %d\n",rf.me,rf.lastApplied)
		rf.log[rf.lastApplied].CommandValid=true
		entry:=rf.log[rf.lastApplied]
		rf.mu.Unlock()
		rf.applicationChan<-entry
		rf.mu.Lock()
	}
}


/*
param: server
Move NextIndex of server to first found index with previous term
*/

func (rf *Raft)NextToPrevTerm(server int){
	prevIndex:=rf.nextIndex[server]
	if prevIndex>=len(rf.log){
		prevIndex=len(rf.log)-1
	}
	curTerm:=rf.log[prevIndex].CommandTerm
	newIndex:=prevIndex
	for ;newIndex>0&&rf.log[newIndex].CommandTerm==curTerm;newIndex--{}
	rf.nextIndex[server]=newIndex
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
			PrevLogTerm:rf.getIndexTerm(rf.nextIndex[i]-1),
			Entries:append([]ApplyMsg{},rf.log[rf.nextIndex[i]:]...),
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
			LastLogIndex:len(rf.log)-1,
			LastLogTerm:rf.getIndexTerm(len(rf.log)-1),
		}
		replies[i]=&RequestVoteReply{}
	}
	return args,replies
}




//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []ApplyMsg
	if d.Decode(&currentTerm) != nil ||d.Decode(&votedFor) != nil ||d.Decode(&log) != nil{
		DPrintf("Loading Error\n")
	} else {
	  	rf.currentTerm = currentTerm
	  	rf.votedFor = votedFor
	  	rf.log=log
	}
}





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
	Entries []ApplyMsg //entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int//leader’s commitIndex
}

type AppendEntriesReply struct{
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
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
		rf.state=3
		rf.SetTermAndVote(args.Term,-1)
		reply.Term=args.Term
	} else{
		reply.Term=rf.currentTerm
	}
	if args.Term>=rf.currentTerm&&(rf.votedFor==-1||rf.votedFor==args.CandidateId)&&rf.upToDate(args.LastLogIndex,args.LastLogTerm){
		reply.VoteGranted=true
		rf.SetVoteFor(args.CandidateId)
		rf.lastAppend=time.Now()
	} else{
		reply.VoteGranted=false
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
		rf.state=3
		rf.SetTermAndVote(args.Term,-1)
		reply.Term=args.Term
	} else if args.Term<rf.currentTerm{
		reply.Term=rf.currentTerm
		reply.Success=false
		return
	} else{
		reply.Term=args.Term
	}

	if rf.getIndexTerm(args.PrevLogIndex)==args.PrevLogTerm{
		i:=0
		for ;i+args.PrevLogIndex+1<len(rf.log)&&i<len(args.Entries);i++{
			if rf.log[i+args.PrevLogIndex+1].CommandTerm!=args.Entries[i].CommandTerm{
				break
			}
		}
		if i<len(args.Entries){
			rf.SetLog(append(rf.log[:i+args.PrevLogIndex+1],args.Entries[i:]...))
		}

		if rf.commitIndex<args.LeaderCommit{
			rf.commitIndex=args.LeaderCommit
		}
		
		if !rf.committing{
			go rf.commit()
		}

		rf.state=3
		reply.Success=true

	} else{
		reply.Success=false
	}
	
	rf.lastAppend=time.Now()


}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
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
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppend", args, reply)
	return ok
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
	index=len(rf.log)+1
	term=rf.currentTerm
	newCommand:=ApplyMsg{
		CommandValid:false,
		Command:command,
		CommandIndex:index,
		CommandTerm:term,
	}
	rf.SetLog(append(rf.log,newCommand))
	DPrintf("*[%d]Leader gets new entry\n",rf.me)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm=0
	rf.votedFor=-1
	rf.log=[]ApplyMsg{}

	//Volatile state on all servers:
	rf.commitIndex=-1
	rf.lastApplied=-1

	//Volatile state on leaders:
	//(Reinitialized after election)
	rf.nextIndex=make([]int,len(rf.peers))
	rf.matchIndex=make([]int,len(rf.peers))

	
	rf.state=3
	rf.lastAppend=time.Now()
	rf.applicationChan=applyCh


	go rf.WaitForResponse()
	DPrintf("================ number of goroutine alive %d================\n",runtime.NumGoroutine())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
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
				go func(server int,arg *AppendEntriesArgs,reply *AppendEntriesReply){
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.killed()||arg.Term!=rf.currentTerm||rf.state!=1{
						return
					}


					DPrintf("[%d] Send append request %d\n",rf.me,server)
					//the request
			
					rf.mu.Unlock()
					ok:=rf.sendAppendEntries(server,arg,reply)
					rf.mu.Lock()
					
					if rf.killed()||rf.state!=1{
						return
					}

					if reply.Term>rf.currentTerm{
						rf.SetTermAndVote(reply.Term,-1)
						rf.state=3
						rf.lastAppend=time.Now()
						go rf.WaitForResponse()
						return
					}

					if ok&&!reply.Success&&rf.nextIndex[server]>0{
						rf.NextToPrevTerm(server)
					} else if ok&&!rf.killed()&&arg.Term==rf.currentTerm&&arg.PrevLogIndex+len(arg.Entries)>rf.matchIndex[server]{
						rf.matchIndex[server]=arg.PrevLogIndex+len(arg.Entries)
						rf.nextIndex[server]=rf.matchIndex[server]+1
						if rf.log[rf.matchIndex[server]].CommandTerm==rf.currentTerm{
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
					
				}(i,args[i],replies[i])
			}
		}
		rf.mu.Unlock()
		time.Sleep(getSendTime())
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
			go func(server int,arg *RequestVoteArgs,reply *RequestVoteReply,voteStatus []bool){
				ok:=false
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for !rf.killed()&&rf.state==2&&rf.currentTerm==arg.Term&&!ok{
					rf.mu.Unlock()
					ok=rf.sendRequestVote(server,arg,reply)
					rf.mu.Lock()

					//check reply term and up to date this check for reply up to date
					if reply.Term>rf.currentTerm{
						rf.SetTermAndVote(reply.Term,-1)
						rf.state=3
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
			}(i,args[i],replies[i],voteStatus)
		}
	}
}

/*
Act as timer for follower/candidate
Check every 80ms and start election if election time passed without message
*/
func (rf *Raft)WaitForResponse(){
	electionTime:=getElectionTime()
	for true{
		rf.mu.Lock()
		if rf.state==1||rf.killed(){
			rf.mu.Unlock()
			return
		}
		if !(rf.state==1||rf.killed())&&time.Since(rf.lastAppend)>=electionTime{
			//start election
			electionTime=getElectionTime()
			rf.state=2
			rf.lastAppend=time.Now()
			go rf.StartElection()
		} 
		rf.mu.Unlock()
		time.Sleep(80*time.Millisecond)
	}
}
