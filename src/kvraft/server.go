package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"bytes"
	"encoding/gob"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//Type 0 is read 1 is put 2 is append 3 is load snapshot
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sender string
	Serial string
	Type int
	Key string
	Value string
	WaitId string
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store map[string]string
	//since clerks are synchronous when a newer request is commited the older request detection can be deleted 
	applied map[string]map[string]bool 
	waiting map[string]chan bool

	waitingNum int

	latestIndex int
	latestTerm int
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	//start raft instance
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store=map[string]string{}
	kv.applied=map[string]map[string]bool{}
	kv.LoadSnapshot(kv.rf.GetLogState().GetSnapshot())
	DPrintf("[%d] persisted store %v\n",kv.me,kv.store)
	DPrintf("[%d] persisted applied %v\n",kv.me,kv.applied)

	kv.waiting=map[string]chan bool{}
	kv.waitingNum=0
	go kv.ProcessCommits()
	return kv
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("[%d] Read received\n",kv.me)
	defer kv.mu.Unlock()
	if term,isLeader:=kv.rf.GetState();!isLeader{
		DPrintf("[%d] defer not leader term %d\n",kv.me,term)
		reply.Ok=false
	} else{
		waitNum:=kv.GetUniqueWaitNum()
		wait:=make(chan bool,1)
		kv.waiting[waitNum]=wait
		op:=Op{
			args.Sender,
			args.Serial,
			0,
			args.Key,
			"",
			waitNum,
			kv.me,
		}
		kv.rf.TryRead(op)
		DPrintf("[%d] Try to apply read",kv.me)
		kv.mu.Unlock()

		timeout:=raft.GetMaxEletionTime()
		select{	
		case <-wait:
			reply.Ok=true
		case <-time.After(timeout):
			reply.Ok=false
		}

		kv.mu.Lock()
		reply.Value=kv.store[args.Key]
		DPrintf("[%d] Done trying to apply read result(%t)",kv.me,reply.Ok)
	}
	return 
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	
	// Your code here.
	kv.mu.Lock()
	DPrintf("[%d] Write received\n",kv.me)
	defer kv.mu.Unlock()
	var action int
	if args.Op=="Put"{
		action=1
	} else if args.Op=="Append"{
		action=2
	}
	if term,isLeader:=kv.rf.GetState();!isLeader{
		DPrintf("[%d] defer not leader term %d\n",kv.me,term)
		reply.Ok=false
	} else{
		waitNum:=kv.GetUniqueWaitNum()
		wait:=make(chan bool,1)
		kv.waiting[waitNum]=wait
		op:=Op{
			args.Sender,
			args.Serial,
			action,
			args.Key,
			args.Value,
			waitNum,
			kv.me,
		}
		kv.rf.Start(op)
		DPrintf("[%d] Try to apply write",kv.me)
		kv.mu.Unlock()

		timeout:=raft.GetMaxEletionTime()
		select{	
		case <-wait:
			reply.Ok=true
		case <-time.After(timeout):
			reply.Ok=false
		}

		kv.mu.Lock()
		DPrintf("[%d] Done trying to apply write result(%t)",kv.me,reply.Ok)
	}
	return 
}



func (kv *KVServer)ProcessCommits(){
	kv.mu.Lock()
	for !kv.killed(){
		kv.mu.Unlock()
		commit:=<-kv.applyCh
		kv.mu.Lock()
		if commit.CommandIndex<=kv.latestIndex{
			continue
		}
		if commit.Type==raft.READ||commit.Type==raft.WRITE{
			command:=commit.Command.(Op)
			DPrintf(
				"[%d] commited command| Sender(%v) Serial(%v),Type(%v),Key(%v),Value(%v),Index(%v)\n",
				kv.me,
				command.Sender,
				command.Serial,
				command.Type,
				command.Key,
				command.Value,
				commit.CommandIndex,
			)

			if commit.Type==raft.READ{

			} else if commit.Type==raft.WRITE{
				kv.processWriteCommand(command)
			}
			if _,ok:=kv.applied[command.Sender][command.Serial];len(kv.applied[command.Sender])>0&&!ok{
				kv.applied[command.Sender]=map[string]bool{command.Serial:true}
			}
			if c,ok:=kv.waiting[command.WaitId];ok{
				c<-true
				DPrintf("[%d] signal channel(%v) to respond to client\n",kv.me,command.WaitId)
				delete(kv.waiting,command.WaitId)
			}
		} else if commit.Type==raft.LOAD{
			command:=commit.Command.(raft.Snapshot)
			kv.LoadSnapshot(&command)
		}
		

		if commit.CommandValid{
			kv.latestIndex=commit.CommandIndex
			kv.latestTerm=commit.CommandTerm
			go kv.Compaction()
		}

	}
	kv.mu.Unlock()
}



func (kv *KVServer) processWriteCommand(command Op){
	if !kv.applied[command.Sender][command.Serial]{
		if command.Type==1{
			kv.store[command.Key]=command.Value
		} else if command.Type==2{
			kv.store[command.Key]+=command.Value
		}
		DPrintf("[%d] applied command %v:(%v)/(%v) Sender[%s] Serial[%s]\n",kv.me,command.Type,command.Key,command.Value,command.Sender,command.Serial)
		kv.applied[command.Sender]=map[string]bool{command.Serial:true}
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	//DPrintf("[%d] %v\n\n\n",kv.me,kv.store)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) GetUniqueWaitNum() string {
	kv.waitingNum+=1
	return fmt.Sprintf("%p%d",kv,kv.waitingNum)
}

func (kv *KVServer) Compaction(){
	if kv.maxraftstate<=0{
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.GetPersister().RaftStateSize()>=kv.maxraftstate{
		snapByte := Encode(kv.store)
		appliedByte:=Encode(kv.applied)
		kv.rf.TryApplicationSetSnapshot(kv.latestIndex,kv.latestTerm,snapByte,appliedByte)
	}
}

func (kv *KVServer) LoadSnapshot(snapshot *raft.Snapshot){
	snapBytes:=snapshot.SnapBytes
	appliedBytes:=snapshot.AppliedBytes
	rSnap:=bytes.NewBuffer(snapBytes)
	dSnap:=gob.NewDecoder(rSnap)
	rApplied:=bytes.NewBuffer(appliedBytes)
	dApplied:=gob.NewDecoder(rApplied)
	var store map[string]string
	var applied map[string]map[string]bool
	
	if dSnap.Decode(&store) != nil || dApplied.Decode(&applied) != nil {
		DPrintf("Loading Error\n")
	} else {
	  	kv.latestIndex=snapshot.LastIndex
		kv.latestTerm=snapshot.LastTerm
		kv.store=store
		kv.applied=applied
	}
}

func Encode(item interface{})[]byte{
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(item)
	return w.Bytes()
}

