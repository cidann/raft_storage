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

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
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

	// Your definitions here.
	store map[string]string
	applied map[string]bool
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store=map[string]string{}
	kv.applied=map[string]bool{}
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
			args.Serial,
			0,
			args.Key,
			"",
			waitNum,
			kv.me,
		}
		kv.rf.TryRead(op)
		go kv.Compaction()
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
			args.Serial,
			action,
			args.Key,
			args.Value,
			waitNum,
			kv.me,
		}
		kv.rf.Start(op)
		go kv.Compaction()
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
		command:=commit.Command.(Op)
		kv.mu.Lock()
		DPrintf(
			"[%d] commited command| Serial(%v),Type(%v),Key(%v),Value(%v),WaitId(%v)\n",
			kv.me,
			command.Serial,
			command.Type,
			command.Key,
			command.Value,
			command.WaitId,
		)
		kv.latestIndex=commit.CommandIndex
		kv.latestTerm=commit.CommandTerm

		if !kv.applied[command.Serial]{
			if command.Type==1{
				kv.store[command.Key]=command.Value
			} else if command.Type==2{
				kv.store[command.Key]+=command.Value
			}
			DPrintf("[%d] applied command %v:(%v)/(%v) Serial[%s]\n",kv.me,command.Type,command.Key,command.Value,command.Serial)
			kv.applied[command.Serial]=true
		}

		if c,ok:=kv.waiting[command.WaitId];ok{
			c<-true
			DPrintf("[%d] signal channel(%v) to respond to client\n",kv.me,command.WaitId)
			delete(kv.waiting,command.WaitId)
		}
	}
	kv.mu.Unlock()
}


func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("[%d] %v\n\n\n",kv.me,kv.store)
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
	if kv.rf.GetPersister().RaftStateSize()>=kv.maxraftstate{
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.store)
		data := w.Bytes()
		newSnapshot:=raft.Snapshot{LastIndex:kv.latestIndex,LastTerm:kv.latestTerm,SnapBytes:data}
		kv.rf.SetSnapshot(&newSnapshot)
	}
}
