package shardkv

import "sync/atomic"

var true_serial_put_append int64 = 0

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Lock(kv, lock_trace, "PutAppend")
	defer Unlock(kv, lock_trace, "PutAppend")
	cur_serial := atomic.AddInt64(&true_serial_put_append, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	Debug(dClient, "G%d <- C%d Received PutAppend Serial:%d Key/Val:{%s:%s} Status:%v true#%d", kv.gid, args.Get_sid(), args.Get_serial(), args.Key, args.Value, kv.state.GetShardsStatus(), cur_serial)
	if kv.state.LatestConfig.Num == 0 || !kv.state.HaveShard(key2shard(args.Key)) {
		reply.Success = false
		if kv.state.LatestConfig.Num != 0 {
			shard_status := kv.state.GetShardsStatus()
			Debug(dInfo, "G%d %v %v %d dont have shard %d from %v config %v", kv.gid, kv.state.Shards[key2shard(args.Key)].Status == OWN, kv.state.LatestConfig.Shards[key2shard(args.Key)] == kv.gid, kv.gid, key2shard(args.Key), shard_status, kv.state.LatestConfig)
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
		var _, received = WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))

	if reply.OutDated {
		Debug(dClient, "G%d <- C%d Outdated PutAppend Serial:%d Key:%s true#%d shards: %v", kv.gid, args.Get_sid(), args.Get_serial(), args.Key, cur_serial, TransformMap(kv.state.Shards, func(ptr *Shard) Shard {
			return *ptr
		}))
	}
	reply.Success = true
	Debug(dClient, "G%d <- C%d Done PutAppend Serial:%d Key:%s Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), args.Get_serial(), args.Key, kv.state.GetShardsStatus(), cur_serial, reply.OutDated)
}
