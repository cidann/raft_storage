package shardkv

import (
	"dsys/raft"
	"dsys/raft_helper"
	"fmt"
)

func (kv *ShardKV) ApplyDaemon() {
	Lock(kv, lock_trace, "ApplyDaemon")
	defer Unlock(kv, lock_trace, "ApplyDaemon")

	for !kv.killed() {
		msg := UnlockUntilChanReceive(kv, kv.applyCh)
		switch msg.Command.(type) {
		case raft_helper.Op:
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

func (kv *ShardKV) handleOperation(msg *raft.ApplyMsg) {
	operation := msg.Command.(raft_helper.Op)
	var side_effect SideEffect = NewNoSideEffect()
	if !kv.state.IsAlreadyProcessed(operation) || operation.Get_type() == GET {
		side_effect = kv.state.DispatchOp(operation)
	}
	kv.state.SetLatest(msg.Index(), msg.Term())
	side_effect.ApplySideEffect(kv)

	//Debug(dCommit, "S%d replicated and applied C%d Serial:%d entry %s %s", kv.me, operation.Sid, operation.Serial, typeMap[operation.Type], operation.Key)
	kv.snapshotCheck(operation)
}

func (kv *ShardKV) snapshotCheck(operation raft_helper.Op) {
	kv.non_snapshot_size += kv.getOperationSize(operation)
	if kv.maxraftstate != -1 && kv.non_snapshot_size >= kv.maxraftstate {
		//log.Printf("[%d] handleOperation %d cur state size %d max size", kv.me, kv.rf.GetStateSize(), kv.maxraftstate)
		//log.Printf("[%d] snapshoted at [index: %d, term: %d] non_snapshot_size %d", kv.me, kv.state.LastIndex, kv.state.LastTerm, kv.non_snapshot_size)
		kv.rf.ApplicationSnapshot(kv.CreateSnapshot(), kv.state.LastIndex, kv.state.LastTerm)
		//log.Printf("[%d] after handleOperation %d cur state size %d max size", kv.me, kv.rf.GetStateSize(), kv.maxraftstate)

		kv.non_snapshot_size = 0
	}
}

func (kv *ShardKV) handleSnapshot(msg *raft.ApplyMsg) {
	if kv.maxraftstate == -1 {
		panic("should not have snapshot when maxraftstate==-1")
	}
	snapshot := msg.Command.(raft.SnapshotData)
	if snapshot.LastIndex > kv.state.LastIndex {
		Debug(dSnap, "S%d handle snapshot from raft index/term: %d/%d", kv.me, snapshot.LastIndex, snapshot.LastTerm)
		kv.LoadSnapshot(snapshot.Data)
		kv.rf.ApplicationSnapshot(snapshot.Data, snapshot.LastIndex, snapshot.LastTerm)
		kv.non_snapshot_size = 0

		if snapshot.LastIndex != kv.state.LastIndex {
			panic(fmt.Sprintf("snapshot index mismatch state index %d snapshot last index %d", kv.state.LastIndex, snapshot.LastIndex))
		}
	}
}
