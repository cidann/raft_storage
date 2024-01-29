package shardmaster

import (
	"dsys/raft"
	"dsys/raft_helper"
	"dsys/sync_helper"
	"fmt"
)

func (sm *ShardMaster) ApplyDaemon() {
	sync_helper.Lock(sm, lock_trace, "ApplyDaemon")
	defer sync_helper.Unlock(sm, lock_trace, "ApplyDaemon")

	for !sm.killed() {
		msg := sync_helper.UnlockUntilChanReceive(sm, sm.applyCh)
		switch msg.Command.(type) {
		case raft_helper.Op:
			sm.handleOperation(&msg)
		case raft.SnapshotData:
			sm.handleSnapshot(&msg)
		case StopDaemon:
			return
		default:
			panic("Unkown command type")
		}
	}
}

func (sm *ShardMaster) handleOperation(msg *raft.ApplyMsg) {
	operation := msg.Command.(raft_helper.Op)
	var op_result Config
	if !sm.tracker.AlreadyProcessed(operation) || operation.Get_type() == QUERY {
		switch operation := operation.(type) {
		case *JoinArgs:
			Debug(dInfo, "S%d handle join sid/serial: %d/%d", sm.me, operation.Get_sid(), operation.Get_serial())
			mapping := operation.Servers
			sm.state.Join(mapping)
		case *LeaveArgs:
			Debug(dInfo, "S%d handle leave sid/serial: %d/%d", sm.me, operation.Get_sid(), operation.Get_serial())
			gids := operation.GIDs
			sm.state.Leave(gids)
		case *MoveArgs:
			Debug(dInfo, "S%d handle move sid/serial: %d/%d", sm.me, operation.Get_sid(), operation.Get_serial())
			shard := operation.Shard
			gid := operation.GID
			sm.state.Move(shard, gid)
		case *QueryArgs:
			Debug(dInfo, "S%d handle query sid/serial: %d/%d", sm.me, operation.Get_sid(), operation.Get_serial())
			cid := operation.Num
			op_result = *sm.state.Query(cid)
		default:
			panic("Unknown op type")
		}
	}

	sm.state.SetLatest(msg.Index(), msg.Term())
	sm.tracker.ProcessRequest(operation, op_result)

	sm.non_snapshot_size += sm.getOperationSize(operation)
	if sm.maxraftstate != -1 && sm.non_snapshot_size >= sm.maxraftstate {
		sm.rf.ApplicationSnapshot(sm.CreateSnapshot(), sm.state.LastIndex, sm.state.LastTerm)
		sm.non_snapshot_size = 0
	}
}

func (sm *ShardMaster) handleSnapshot(msg *raft.ApplyMsg) {
	if sm.maxraftstate == -1 {
		panic("should not have snapshot when maxraftstate==-1")
	}
	snapshot := msg.Command.(raft.SnapshotData)
	if snapshot.LastIndex > sm.state.LastIndex {
		Debug(dSnap, "S%d handle snapshot from raft index/term: %d/%d", sm.me, snapshot.LastIndex, snapshot.LastTerm)
		sm.LoadSnapshot(snapshot.Data)
		sm.rf.ApplicationSnapshot(snapshot.Data, snapshot.LastIndex, snapshot.LastTerm)
		sm.non_snapshot_size = 0

		if snapshot.LastIndex != sm.state.LastIndex {
			panic(fmt.Sprintf("snapshot index mismatch state index %d snapshot last index %d", sm.state.LastIndex, snapshot.LastIndex))
		}
	}
}
