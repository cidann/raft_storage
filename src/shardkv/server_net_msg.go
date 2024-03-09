package shardkv

import (
	"dsys/raft"
	"dsys/shardmaster"
	"sync"
)

type NetworkMessage interface {
	SendNetworkMessage(kv *ShardKV)
}

type SendTransferDecision struct {
	config     shardmaster.Config
	source_gid int
	target_gid int
}

func NewSendTransferDecision(config shardmaster.Config, target_gid, source_gid int) *SendTransferDecision {
	return &SendTransferDecision{
		config:     config,
		target_gid: target_gid,
		source_gid: source_gid,
	}
}

func (side_effect *SendTransferDecision) SendNetworkMessage(kv *ShardKV) {
	Debug(dInfo, "G%d leader: %v SendTransferDecision Network msg config num %d", kv.gid, kv.IsLeader(), side_effect.config.Num)
	if kv.IsLeader() {
		go kv.clerk_pool.AsyncTransferShardsDecision(side_effect.target_gid, side_effect.source_gid, side_effect.config)
	}
}

func (kv *ShardKV) PersistNetDaemon() {
	Lock(kv, lock_trace, "PersistNetDaemon")
	defer Unlock(kv, lock_trace, "PersistNetDaemon")
	Debug(dTrace, "G%d PersistNetDaemon Start", kv.gid)

	Assert(kv.state.LatestConfig.Num > 0, "Uninitialized config")
	for !kv.killed() {
		if kv.IsLeader() && !kv.state.AreShardsStable() {
			to_transfer := TransformMap(kv.GetShardsToTransfer(), func(shards []Shard) []Shard {
				copy_shards := []Shard{}
				for _, shard := range shards {
					copy_shards = append(copy_shards, *shard.Copy())
				}
				return copy_shards
			})
			config := *kv.state.LatestConfig
			if len(to_transfer) > 0 {
				Debug(dInfo, "G%d retry transfer shard %v", kv.gid, to_transfer)
				UnlockUntilFunc(kv, func() { kv.SendTransferShardMessages(to_transfer, config, kv.gid) })
			}
		}
		UnlockAndSleepFor(kv, raft.GetSendTime()*20)
	}
	Debug(dTrace, "G%d PersistNetDaemon Stopped", kv.gid)
}

func (kv *ShardKV) GetShardsToTransfer() map[int][]Shard {
	to_transfer := map[int][]Shard{}
	for shard_num, gid := range kv.state.LatestConfig.Shards {
		Assert(kv.state.Shards[shard_num] != nil, "Nil shard %d %v", shard_num, kv.state.Shards)
		if gid != kv.gid && kv.state.Shards[shard_num].Status == PUSHING {
			to_transfer[gid] = append(to_transfer[gid], *kv.state.Shards[shard_num])
		}
	}
	return to_transfer
}

func (kv *ShardKV) SendTransferShardMessages(to_transfer map[int][]Shard, config shardmaster.Config, source_gid int) {
	wg := sync.WaitGroup{}
	for target_gid, shards := range to_transfer {
		wg.Add(1)
		go func(target_gid int, shards []Shard) {
			kv.clerk_pool.AsyncTransferShards(target_gid, source_gid, config, shards)
			wg.Done()
		}(target_gid, shards)
	}
	wg.Wait()
}
