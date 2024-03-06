package shardkv

// import "dsys/shardmaster"
import (
	"bytes"
	"dsys/labgob"
	"dsys/raft_helper"
	"dsys/shardmaster"
	"dsys/sync_helper"
	"fmt"
	"sync"
	"sync/atomic"

	"dsys/labrpc"
	"dsys/raft"
)

type StopDaemon int

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state      *ServerState
	mck        *shardmaster.Clerk
	clerk_pool *ClerkPool

	num_raft          int
	non_snapshot_size int   //size of entries that is commited but not discarded from snapshot
	dead              int32 // set by Kill()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	Debug(dDrop, "S%d kill kv", kv.me)
	kv.rf.Kill()
	Lock(kv, false)
	defer Unlock(kv, false)
	UnlockUntilChanSend(kv, kv.applyCh, raft.ApplyMsg{Command: StopDaemon(1)})
	kv.state.DiscardAllRequest()
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.clerk_pool = NewClerkPool(masters, make_end)

	kv.non_snapshot_size = 0
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.state = NewServerState(kv.gid)
	kv.num_raft = len(servers)
	kv.LoadSnapshot(persister.ReadSnapshot())
	Debug(dInit, "G%d server created", gid)

	go kv.InitDaemon()

	return kv
}

func (kv *ShardKV) InitDaemon() {
	kv.InitConfig()
	go kv.ApplyDaemon()
	go kv.ConfigPullDaemon()
	go kv.PersistNetDaemon()
}

func (kv *ShardKV) GetLeader() (int, bool) {
	_, leader, isLeader := kv.rf.GetStateAndLeader()
	return leader, isLeader
}

func (kv *ShardKV) IsLeader() bool {
	_, is_leader := kv.GetLeader()
	return is_leader
}

func (kv *ShardKV) Lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) Unlock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) Identity() string {
	return fmt.Sprintf("KV server: [%d]", kv.me)
}

func (kv *ShardKV) StartSetOp(args raft_helper.Op, reply raft_helper.Reply) bool {
	success := true

	result_chan := make(chan any, 1)
	kv.state.RecordRequestGeneral(args, result_chan)
	start_and_wait := func() {
		kv.rf.Start(args)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.Set_outDated(!received)
		success = received
	}
	sync_helper.UnlockUntilChanReceive(kv, sync_helper.GetChanForFunc[any](start_and_wait))

	return success
}

func (kv *ShardKV) GetId() int {
	return kv.me
}

func (kv *ShardKV) Initialized() bool {
	return kv.state != nil && kv.state.LatestConfig != nil && kv.state.LatestConfig.Num != 0
}

func (kv *ShardKV) CreateSnapshot() []byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	kv.state.EncodeData(*encoder)
	return buf.Bytes()
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)
	decoder.Decode(kv.state)
	kv.state.Recover()
}

func (kv *ShardKV) getOperationSize(operation raft_helper.Op) int {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(operation)
	return len(buf.Bytes())
}
