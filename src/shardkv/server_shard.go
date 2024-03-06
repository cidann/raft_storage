package shardkv

import (
	"dsys/raft_helper"
	"sync/atomic"
)

var true_serial_shard_transfer int64 = 0
var true_serial_shard_transfer_decision int64 = 0

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	Lock(kv, lock_trace, "TransferShard")
	defer Unlock(kv, lock_trace, "TransferShard")

	cur_serial := atomic.AddInt64(&true_serial_shard_transfer, 1)

	Debug(dClient, "G%d <- C%d Received Transfer Shard Serial:%d as Leader true#%d", kv.gid, args.Get_sid(), args.Get_serial(), cur_serial)

	raft_helper.HandleStateChangeRPC(kv, "TransferShard", args, reply)

	Debug(dClient, "G%d <- C%d Received Transfer Shard Serial:%d done true#%d Outdated:%t", kv.gid, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) TransferShardDecision(args *ShardReceivedArgs, reply *ShardReceivedReply) {
	Lock(kv, lock_trace, "TransferShardDecision")
	defer Unlock(kv, lock_trace, "TransferShardDecision")

	cur_serial := atomic.AddInt64(&true_serial_shard_transfer_decision, 1)

	Debug(dClient, "G%d <- C%d Received Transfer Shard Decision Serial:%d as Leader true#%d", kv.gid, args.Get_sid(), args.Get_serial(), cur_serial)

	raft_helper.HandleStateChangeRPC(kv, "TransferShardDecision", args, reply)

	Debug(dClient, "G%d <- C%d Received Transfer Shard Decision Status Now: %v Serial:%d done true#%d Outdated:%t", kv.gid, args.Get_sid(), kv.state.GetShardsStatus(), args.Get_serial(), cur_serial, reply.Get_outDated())
}
