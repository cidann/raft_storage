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

	Debug(dClient, "G%d <- C%d Received Transfer Shards %d Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), shard_nums, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done Transfer Shards %d Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), shard_nums, args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) TransferShardDecision(args *ShardReceivedArgs, reply *ShardReceivedReply) {
	Lock(kv, lock_trace, "TransferShardDecision")
	defer Unlock(kv, lock_trace, "TransferShardDecision")

	cur_serial := atomic.AddInt64(&true_serial_shard_transfer_decision, 1)

	Debug(dClient, "G%d <- C%d Received Transfer Shard Decision Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done Transfer Shard Decision Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())
}
