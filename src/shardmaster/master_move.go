package shardmaster

import (
	"dsys/labgob"
	"sync/atomic"
)

type MoveOperationArgs struct {
	Shard int
	GID   int
}

func init() {
	labgob.Register(MoveOperationArgs{})
}

var true_serial_move int64 = 0

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	Lock(sm, lock_trace, "Move")
	defer Unlock(sm, lock_trace, "Move")
	cur_serial := atomic.AddInt64(&true_serial_move, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Move Serial:%d Shard/GID: %v/%v as Leader true#%d", sm.me, args.Sid, args.Serial, args.Shard, args.GID, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   MOVE,
		Args:   MoveOperationArgs{Shard: args.Shard, GID: args.GID},
	}
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var _, received = WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	UnlockUntilChanReceive(sm, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Move Serial:%d Shard/GID: %v/%v done true#%d Outdated:%t", sm.me, args.Sid, args.Serial, args.Shard, args.GID, cur_serial, reply.OutDated)

}
