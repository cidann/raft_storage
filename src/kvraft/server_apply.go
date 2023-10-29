package kvraft

import (
	"dsys/raft"
	"fmt"
)

func (kv *KVServer) ApplyDaemon() {
	Lock(kv, lock_trace, "ApplyDaemon")
	defer Unlock(kv, lock_trace, "ApplyDaemon")

	for !kv.killed() {
		msg := UnlockUntilChanReceive(kv, kv.applyCh)
		switch msg.Command.(type) {
		case Op:
			kv.handleOperation(&msg)
		case raft.SnapshotData:
			kv.handleSnapshot(&msg)
		case StopDaemon:
			return
		default:
			panic("Unkown command type")
		}
	}
}

func (kv *KVServer) handleOperation(msg *raft.ApplyMsg) {
	DPrintf("[%d] handleOperation", kv.me)
	operation := msg.Command.(Op)
	op_result := ""
	if !kv.tracker.AlreadyProcessed(&operation) || operation.Type == GET {
		op_result = kv.state.Apply(operation, kv.me)
		//DPrintf("[%d] state after entry %s", kv.me, kv.state.KvState)
	}
	kv.state.SetLatest(msg.Index(), msg.Term())
	kv.tracker.ProcessRequest(&operation, op_result)

	kv.non_snapshot_size += kv.getOperationSize(&operation)
	if kv.maxraftstate != -1 && kv.non_snapshot_size >= kv.maxraftstate*2 {
		//log.Printf("[%d] handleOperation %d cur state size %d max size", kv.me, kv.rf.GetStateSize(), kv.maxraftstate)
		//log.Printf("[%d] snapshoted at [index: %d, term: %d] non_snapshot_size %d", kv.me, kv.state.LastIndex, kv.state.LastTerm, kv.non_snapshot_size)
		kv.rf.ApplicationSnapshot(kv.CreateSnapshot(), kv.state.LastIndex, kv.state.LastTerm)
		//log.Printf("[%d] after handleOperation %d cur state size %d max size", kv.me, kv.rf.GetStateSize(), kv.maxraftstate)

		kv.non_snapshot_size = 0
	}
}

func (kv *KVServer) handleSnapshot(msg *raft.ApplyMsg) {
	if kv.maxraftstate == -1 {
		panic("should not have snapshot when maxraftstate==-1")
	}
	snapshot := msg.Command.(raft.SnapshotData)
	if snapshot.LastIndex > kv.state.LastIndex {
		kv.LoadSnapshot(snapshot.Data)
	}
	if snapshot.LastIndex != kv.state.LastIndex {
		panic(fmt.Sprintf("snapshot index mismatch state index %d snapshot last index %d", kv.state.LastIndex, snapshot.LastIndex))
	}
}
