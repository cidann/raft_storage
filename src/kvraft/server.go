package kvraft

import (
	"dsys/labgob"
	"dsys/labrpc"
	"dsys/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"bytes"
	"encoding/gob"
)

/*
Lock for application layer is at
RPCs
ApplyDaemon that accepts replicated changes and might wake up waiting RPCs
*/

type OperationType int

const (
	GET OperationType = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Serial int
	Sid    int
	Type   OperationType
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	tracker      *RequestTracker
	state        *ServerState


	num_raft int
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	/*
		kv.rf.Kill()
		for i := 0; i < len(kv.tracker.request_chan); i++ {
			if kv.tracker.request_chan[i] != nil {
				close(kv.tracker.request_chan[i])
			}
		}
		raft.DPrintfl2("[%d] killed %d", kv.me, kv.num_raft)
	*/

}


	latestIndex int
	latestTerm int
}

func (kv *KVServer) GetLeader() (int, bool) {
	_, leader, isLeader := kv.rf.GetStateAndLeader()
	return leader, isLeader
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
	kv.tracker = NewRequestTracker()
	kv.state = NewServerState()
	kv.num_raft = len(servers)
	go kv.ApplyDaemon()

	// You may need initialization code here.
	kv.store=map[string]string{}
	kv.applied=map[string]bool{}
	kv.waiting=map[string]chan bool{}
	kv.waitingNum=0
	go kv.ProcessCommits()
	return kv
}


func (kv *KVServer) PingDebug(args *GetArgs, reply *GetReply) {
	Lock(kv, lock_trace)
	defer Unlock(kv, lock_trace)

	DPrintf("[%d to %d]", args.Sid, kv.me)
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) Identity() string {
	return fmt.Sprintf("[%d]", kv.me)

}
