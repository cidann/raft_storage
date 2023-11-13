package kvraft

import (
	"bytes"
	"dsys/labgob"
	"dsys/labrpc"
	"dsys/raft"
	"fmt"
	"sync"
	"sync/atomic"
)

/*
Lock for application layer is at
RPCs
ApplyDaemon that accepts replicated changes and might wake up waiting RPCs
*/

type OperationType int
type StopDaemon int

const (
	GET OperationType = iota
	PUT
	APPEND
)

var typeMap = map[OperationType]string{
	GET:    "Get",
	PUT:    "Put",
	APPEND: "Append",
}

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

	tracker *RequestTracker
	state   *ServerState

	num_raft          int
	non_snapshot_size int //size of entries that is commited but not discarded from snapshot
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
	Debug(dDrop, "S%d kill kv", kv.me)
	kv.rf.Kill()
	Lock(kv, false)
	defer Unlock(kv, false)
	UnlockUntilChanSend(kv, kv.applyCh, raft.ApplyMsg{Command: StopDaemon(1)})
	for k := range kv.tracker.request_chan {
		kv.tracker.DiscardRequestFrom(k)
	}
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.non_snapshot_size = 0
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.tracker = NewRequestTracker()
	kv.state = NewServerState()
	kv.num_raft = len(servers)
	kv.LoadSnapshot(persister.ReadSnapshot())
	go kv.ApplyDaemon()

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) PingDebug(args *GetArgs, reply *GetReply) {
	Lock(kv, lock_trace)
	defer Unlock(kv, lock_trace)

	Debug(dClient, "S%d <- C%d", kv.me, args.Sid)
}

func (kv *KVServer) Lock() {
	kv.mu.Lock()
}

func (kv *KVServer) Unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) Identity() string {
	return fmt.Sprintf("KV server: [%d]", kv.me)
}

func (kv *KVServer) CreateSnapshot() []byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(*kv.state)
	encoder.Encode(kv.tracker.latest_applied)
	encoder.Encode(kv.tracker.request_serial)
	return buf.Bytes()
}

func (kv *KVServer) LoadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	decoder.Decode(kv.state)
	decoder.Decode(&kv.tracker.latest_applied)
	decoder.Decode(&kv.tracker.request_serial)
	for cid := range kv.tracker.request_chan {
		kv.tracker.DiscardRequestFrom(cid)
	}
}

func (kv *KVServer) getOperationSize(operation *Op) int {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(*operation)
	return len(buf.Bytes())
}
