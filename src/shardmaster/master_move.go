package shardmaster

import (
	"dsys/sync_helper"
	"sync/atomic"
)

var true_serial_move int64 = 0

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sync_helper.Lock(sm, lock_trace, "Move")
	defer sync_helper.Unlock(sm, lock_trace, "Move")
	cur_serial := atomic.AddInt64(&true_serial_move, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Move Serial:%d Shard/GID: %v/%v as Leader true#%d", sm.me, args.Get_sid(), args.Get_serial(), args.Shard, args.GID, cur_serial)

	operation := args
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	sync_helper.UnlockUntilChanReceive(sm, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Move Serial:%d Shard/GID: %v/%v done true#%d Outdated:%t", sm.me, args.Get_sid(), args.Get_serial(), args.Shard, args.GID, cur_serial, reply.OutDated)

}
