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
	Debug(dClient, "G%d <- C%d Received Get Config:%d Serial:%d Key:%s Status:%v true#%d", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), args.Key, kv.state.GetShardsStatus(), cur_serial)
	if kv.state.LatestConfig.Num == 0 || !kv.state.HaveShard(key2shard(args.Key)) {
		reply.Success = false
		if kv.state.LatestConfig.Num != 0 {
			shard_status := kv.state.GetShardsStatus()
			Debug(dInfo, "G%d %v %v dont have shard %d from %v config %v", kv.gid, kv.state.Shards[key2shard(args.Key)].Status == OWN, kv.state.LatestConfig.Shards[key2shard(args.Key)] == kv.gid, key2shard(args.Key), shard_status, kv.state.LatestConfig)
		} else {
			Debug(dInfo, "G%d unitialized config", kv.gid)
		}
		return
	}

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
	Debug(dClient, "G%d <- C%d Done Get Config:%d Serial:%d Key/Val:{%s:%s} Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), args.Key, reply.Value, kv.state.GetShardsStatus(), cur_serial, reply.OutDated)

}
