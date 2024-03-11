package shardkv

import (
	"dsys/raft"
	"dsys/raft_helper"
	"dsys/shardmaster"
	"sync"
)

func (kv *ShardKV) ShardTransferDaemon() {
	Lock(kv, lock_trace, "PersistNetDaemon")
	defer Unlock(kv, lock_trace, "PersistNetDaemon")
	Debug(dTrace, "G%d PersistNetDaemon Start", kv.gid)

	Assert(kv.state.LatestConfig.Num > 0, "Uninitialized config")
	for !kv.killed() {
		if kv.IsLeader() && !kv.state.AreShardsStable() {
			to_transfer := kv.GetShardsToTransfer()
			config := *kv.state.LatestConfig
			if len(to_transfer) > 0 {
				Debug(dInfo, "G%d retry transfer shard %v", kv.gid, to_transfer)
				UnlockUntilFunc(kv, func() { kv.SendTransferShardMessages(to_transfer, config, kv.gid) })
			}
		}
		UnlockAndSleepFor(kv, raft.GetSendTime())
	}
	Debug(dTrace, "G%d PersistNetDaemon Stopped", kv.gid)
}

func (kv *ShardKV) ReplicateTransferReceived(receiver_gid int, config *shardmaster.Config) {
	clerk := kv.clerk_pool.GetClerk()
	defer kv.clerk_pool.PutClerk(clerk)

	clerk.serial += 1

	args := ShardReceivedArgs{
		Op:           raft_helper.NewOpBase(clerk.serial, clerk.id, TRANSFERSHARDDECISION),
		Config:       *config,
		Receiver_Gid: receiver_gid,
	}
	for kv.IsLeader() {
		reply := ShardReceivedReply{
			*raft_helper.NewReplyBase(),
		}
		kv.TransferShardDecision(&args, &reply)
		if reply.Success && !reply.OutDated {
			return
		}
	}
}

func (kv *ShardKV) GetShardsToTransfer() map[int][]Shard {
	to_transfer := map[int][]Shard{}
	for shard_num, gid := range kv.state.LatestConfig.Shards {
		Assert(kv.state.Shards[shard_num] != nil, "Nil shard %d %v", shard_num, kv.state.Shards)
		if gid != kv.gid && kv.state.Shards[shard_num].Status == PUSHING {
			to_transfer[gid] = append(to_transfer[gid], *kv.state.Shards[shard_num])
		}
	}
	to_transfer = TransformMap(to_transfer, func(shards []Shard) []Shard {
		copy_shards := []Shard{}
		for _, shard := range shards {
			copy_shards = append(copy_shards, *shard.Copy())
		}
		return copy_shards
	})
	return to_transfer
}

func (kv *ShardKV) SendTransferShardMessages(to_transfer map[int][]Shard, config shardmaster.Config, source_gid int) {
	wg := sync.WaitGroup{}
	for target_gid, shards := range to_transfer {
		wg.Add(1)
		go func(target_gid int, shards []Shard) {
			kv.clerk_pool.AsyncTransferShards(target_gid, source_gid, config, shards)
			kv.ReplicateTransferReceived(target_gid, &config)
			wg.Done()
		}(target_gid, shards)
	}
	wg.Wait()
}
