package shardkv

import "sync/atomic"

var true_serial_get int64 = 0

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	Lock(kv, lock_trace, "Get")
	defer Unlock(kv, lock_trace, "Get")
	cur_serial := atomic.AddInt64(&true_serial_get, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	if !kv.state.OwnShard(key2shard(args.Key)) {
		reply.Success = false
		return
	}

	Debug(dClient, "S%d <- C%d Received Get Serial:%d Key:%s as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), args.Key, cur_serial)

	operation := args
	result_chan := make(chan any, 1)

	kv.state.RecordRequestShard(key2shard(args.Key), operation, result_chan)
	start_and_wait := func() {
		kv.rf.Start(operation)
		val, received := WaitUntilChanReceive(result_chan)
		if !received {
			reply.Value = "closed"
			reply.OutDated = true
		} else {
			reply.Value = val.(string)
			reply.OutDated = false
		}
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Get Serial:%d Key/Val:{%s:%s} done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), args.Key, reply.Value, cur_serial, reply.OutDated)

}