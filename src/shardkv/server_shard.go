package shardkv

import (
	"dsys/sync_helper"
	"sync/atomic"
)

var true_serial_shard_transfer int64 = 0
var true_serial_shard_transfer_decision int64 = 0

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	sync_helper.Lock(kv, lock_trace, "TransferShard")
	defer sync_helper.Unlock(kv, lock_trace, "TransferShard")
	cur_serial := atomic.AddInt64(&true_serial_shard_transfer, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Transfer Shard Serial:%d as Leader true#%d", kv.me, args.Sid, args.Serial, cur_serial)

	result_chan := make(chan any, 1)

	kv.state.RecordRequestGeneral(args, result_chan)
	start_and_wait := func() {
		kv.rf.Start(args)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	sync_helper.UnlockUntilChanReceive(kv, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Received Transfer Shard Serial:%d done true#%d Outdated:%t", kv.me, args.Sid, args.Serial, cur_serial, reply.OutDated)
}

func (kv *ShardKV) TransferShardDecision(args *TransferShardDecisionArgs, reply *TransferShardDecisionReply) {
	sync_helper.Lock(kv, lock_trace, "TransferShardDecision")
	defer sync_helper.Unlock(kv, lock_trace, "TransferShardDecision")
	cur_serial := atomic.AddInt64(&true_serial_shard_transfer_decision, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Transfer Shard Decision Serial:%d as Leader true#%d", kv.me, args.Sid, args.Serial, cur_serial)

	result_chan := make(chan any, 1)

	kv.state.RecordRequestGeneral(args, result_chan)
	start_and_wait := func() {
		kv.rf.Start(args)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	sync_helper.UnlockUntilChanReceive(kv, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Received Transfer Shard Decision Serial:%d done true#%d Outdated:%t", kv.me, args.Sid, args.Serial, cur_serial, reply.OutDated)
}
