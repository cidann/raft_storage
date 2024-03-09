package shardkv

import (
	"sync/atomic"
	"time"
)

var true_serial_shard_transfer int64 = 0
var true_serial_shard_transfer_decision int64 = 0

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	Lock(kv, lock_trace, "TransferShard")
	defer Unlock(kv, lock_trace, "TransferShard")

	cur_serial := atomic.AddInt64(&true_serial_shard_transfer, 1)
	shard_nums := Transform(args.Shards, func(shard Shard) int { return shard.ShardNum })

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}
	Debug(dClient, "G%d <- C%d Received Transfer Shards %d Config:%d Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), shard_nums, kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done Transfer Shards %d Config:%d Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), shard_nums, kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) TransferShardDecision(args *ShardReceivedArgs, reply *ShardReceivedReply) {
	Lock(kv, lock_trace, "TransferShardDecision")
	defer Unlock(kv, lock_trace, "TransferShardDecision")

	cur_serial := atomic.AddInt64(&true_serial_shard_transfer_decision, 1)

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}
	Debug(dClient, "G%d <- C%d Received Transfer Shard Decision Config:%d Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done Transfer Shard Config:%d Decision Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), kv.state.LatestConfig.Num, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())
}
