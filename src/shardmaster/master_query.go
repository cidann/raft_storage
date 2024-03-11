package shardmaster

import (
	"dsys/sync_helper"
	"sync/atomic"
)

var true_serial_query int64 = 0

/*
We don't need to replicate the read if the requested config num is
lower than the already replicated number.
This is similar reasoning to MVCC where config num acts as timestamp
since state in past timestamp won't change so its block/replication free
In config terms, config of the past wont change(each configs version are immutable)
and can only be changed through new version of config
*/
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sync_helper.Lock(sm, lock_trace, "Query")
	defer sync_helper.Unlock(sm, lock_trace, "Query")
	cur_serial := atomic.AddInt64(&true_serial_query, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	if sm.state.LatestConfig().Num <= args.Num {
		reply.Config = *sm.state.LatestConfig()
		reply.Success = true
		return
	}

	Debug(dClient, "S%d <- C%d Received Query Serial:%d Num: %v as Leader true#%d", sm.me, args.Get_sid(), args.Get_serial(), args.Num, cur_serial)

	operation := args
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var config, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
		reply.Config = config
	}
	sync_helper.UnlockUntilChanReceive(sm, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true

	Debug(dClient, "S%d <- C%d Query Serial:%d Num: %v Config:%+v done true#%d Outdated:%t", sm.me, args.Get_sid(), args.Get_serial(), args.Num, reply.Config, cur_serial, reply.OutDated)
}
