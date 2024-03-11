package shardkv

import (
	"sync/atomic"
	"time"
)

var true_serial_no_op int64

func (kv *ShardKV) NoOp(args *NoOpArgs, reply *NoOpReply) {
	Lock(kv, lock_trace, "NoOp")
	defer Unlock(kv, lock_trace, "NoOp")

	cur_serial := atomic.AddInt64(&true_serial_no_op, 1)

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}
	Debug(dClient, "G%d <- C%d Received No Op Config:%d Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done No Op Config:%d Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())

}
