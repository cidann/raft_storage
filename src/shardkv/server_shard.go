package shardkv

import (
	"dsys/raft_helper"
	"sync/atomic"
)

var true_serial_shard_transfer int64 = 0
var true_serial_shard_transfer_decision int64 = 0

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	cur_serial := atomic.AddInt64(&true_serial_shard_transfer, 1)

	Debug(dClient, "S%d <- C%d Received Transfer Shard Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)

	raft_helper.HandleStateChangeRPC(kv, "TransferShard", args, reply)

	Debug(dClient, "S%d <- C%d Received Transfer Shard Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) TransferShardDecision(args *TransferShardDecisionArgs, reply *TransferShardDecisionReply) {
	cur_serial := atomic.AddInt64(&true_serial_shard_transfer_decision, 1)

	Debug(dClient, "S%d <- C%d Received Transfer Shard Decision Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)

	raft_helper.HandleStateChangeRPC(kv, "TransferShardDecision", args, reply)

	Debug(dClient, "S%d <- C%d Received Transfer Shard Decision Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}
