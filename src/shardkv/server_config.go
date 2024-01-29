package shardkv

import (
	"dsys/raft"
	"dsys/sync_helper"
	"sync/atomic"
)

var true_serial_new_config int64 = 0

func (kv *ShardKV) NewConfig(args *NewConfigArgs, reply *NewConfigReply) {
	sync_helper.Lock(kv, lock_trace, "NewConfig")
	defer sync_helper.Unlock(kv, lock_trace, "NewConfig")
	cur_serial := atomic.AddInt64(&true_serial_new_config, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received New Config Serial:%d as Leader true#%d", kv.me, args.Sid, args.Serial, cur_serial)

	result_chan := make(chan any, 1)

	kv.state.RecordRequestGeneral(args, result_chan)
	start_and_wait := func() {
		kv.rf.Start(args)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	sync_helper.UnlockUntilChanReceive(kv, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d New Config  Serial:%d done true#%d Outdated:%t", kv.me, args.Sid, args.Serial, cur_serial, reply.OutDated)

}

func (kv *ShardKV) ConfigPullDaemon() {
	Lock(kv, lock_trace, "ConfigPullDaemon")
	defer Unlock(kv, lock_trace, "ConfigPullDaemon")

	for !kv.killed() {
		if _, isLeader := kv.GetLeader(); isLeader {
			config := kv.mck.Query(-1)
			go kv.clerk_pool.AsyncNewConfig(kv.gid, config)
		}
		UnlockAndSleepFor(kv, raft.GetSendTime()*10)
	}
}
