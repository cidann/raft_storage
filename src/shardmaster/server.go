package shardmaster

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"

	"dsys/labgob"

	"dsys/labrpc"
	"dsys/raft"
	"dsys/sync_helper"
)

type OperationType int

const (
	JOIN OperationType = iota
	LEAVE
	MOVE
	QUERY
)

var typeMap = map[OperationType]string{
	JOIN:  "JOIN",
	LEAVE: "LEAVE",
	MOVE:  "MOVE",
	QUERY: "QUERY",
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	state   *ConfigState
	tracker *RequestTracker

	maxraftstate      int
	non_snapshot_size int
	dead              int64
}

type Op struct {
	Serial int
	Sid    int
	Type   OperationType
	Args   interface{}
}

type StopDaemon int

func (sm *ShardMaster) GetLeader() (int, bool) {
	_, leader, isLeader := sm.rf.GetStateAndLeader()
	return leader, isLeader
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sync_helper.Lock(sm, false)
	defer sync_helper.Unlock(sm, false)
	Debug(dDrop, "S%d kill shardmaster", sm.me)
	sm.rf.Kill()
	atomic.StoreInt64(&sm.dead, 1)

	sync_helper.UnlockUntilChanSend(sm, sm.applyCh, raft.ApplyMsg{Command: StopDaemon(1)})
	for k := range sm.tracker.request_chan {
		sm.tracker.DiscardRequestFrom(k)
	}
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt64(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.tracker = NewRequestTracker()
	sm.state = NewConfigState()
	sm.dead = 0
	sm.maxraftstate = -1
	sm.non_snapshot_size = 0

	go sm.ApplyDaemon()

	return sm
}

func (sm *ShardMaster) Lock() {
	sm.mu.Lock()
}

func (sm *ShardMaster) Unlock() {
	sm.mu.Unlock()
}

func (sm *ShardMaster) Identity() string {
	return "S" + strconv.Itoa(sm.me)
}

func (sm *ShardMaster) CreateSnapshot() []byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(*sm.state)
	encoder.Encode(sm.tracker.latest_applied)
	encoder.Encode(sm.tracker.request_serial)
	return buf.Bytes()
}

func (sm *ShardMaster) LoadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	decoder.Decode(sm.state)
	decoder.Decode(&sm.tracker.latest_applied)
	decoder.Decode(&sm.tracker.request_serial)
	for cid := range sm.tracker.request_chan {
		sm.tracker.DiscardRequestFrom(cid)
	}
}

func (sm *ShardMaster) getOperationSize(operation *Op) int {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(*operation)
	return len(buf.Bytes())
}
