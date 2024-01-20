package shardmaster

import (
	"dsys/raft"
	"fmt"
)

func (sm *ShardMaster) ApplyDaemon() {
	Lock(sm, lock_trace, "ApplyDaemon")
	defer Unlock(sm, lock_trace, "ApplyDaemon")

	for !sm.killed() {
		msg := UnlockUntilChanReceive(sm, sm.applyCh)
		switch msg.Command.(type) {
		case Op:
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
	operation := msg.Command.(Op)
	var op_result Config
	if !sm.tracker.AlreadyProcessed(&operation) || operation.Type == QUERY {
		switch operation.Type {
		case JOIN:
			mapping := operation.Args.(JoinOperationArgs).Server
			sm.state.Join(mapping)
		case LEAVE:
			gids := operation.Args.(LeaveOperationArgs).GIDs
			sm.state.Leave(gids)
		case MOVE:
			shard := operation.Args.(MoveOperationArgs).Shard
			gid := operation.Args.(MoveOperationArgs).GID
			sm.state.Move(shard, gid)
		case QUERY:
			cid := operation.Args.(QueryOperationArgs).Num
			op_result = *sm.state.Query(cid)
		default:
			panic("Unknown op type")
		}
	}
	sm.state.SetLatest(msg.Index(), msg.Term())
	sm.tracker.ProcessRequest(&operation, op_result)

	sm.non_snapshot_size += sm.getOperationSize(&operation)
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
