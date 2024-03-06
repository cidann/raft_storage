package shardkv

import (
	"dsys/raft"
	"dsys/shardmaster"
)

type NetworkMessage interface {
	SendNetworkMessage(kv *ShardKV)
}

type SendTransferDecision struct {
	config     shardmaster.Config
	source_gid int
	target_gid int
}

type SendTransferShards struct {
	config          shardmaster.Config
	transfer_shards map[int][]Shard
	source_gid      int
}

func (side_effect *SendTransferShards) SendNetworkMessage(kv *ShardKV) {
	Debug(dInfo, "G%d leader: %v SendTransferShards Network msg %v", kv.gid, kv.IsLeader(), side_effect.transfer_shards)
	if kv.IsLeader() {
		for target_gid, shards := range side_effect.transfer_shards {
			copy_shards := []Shard{}
			for _, shard := range shards {
				copy_shards = append(copy_shards, *shard.Copy())
			}
			go kv.clerk_pool.AsyncTransferShards(target_gid, side_effect.source_gid, side_effect.config, copy_shards)
		}
	}
}

func (side_effect *SendTransferDecision) SendNetworkMessage(kv *ShardKV) {
	Debug(dInfo, "G%d leader: %v SendTransferDecision Network msg config num %d", kv.gid, kv.IsLeader(), side_effect.config.Num)
	if kv.IsLeader() {
		go kv.clerk_pool.AsyncTransferShardsDecision(side_effect.target_gid, side_effect.source_gid, side_effect.config)
	}
}

func NewSendTransferDecision(config shardmaster.Config, target_gid, source_gid int) *SendTransferDecision {
	return &SendTransferDecision{
		config:     config,
		target_gid: target_gid,
		source_gid: source_gid,
	}
}

func NewSendTransferShard(transfer_shards map[int][]Shard, config shardmaster.Config, source_gid int) *SendTransferShards {
	return &SendTransferShards{
		transfer_shards: transfer_shards,
		config:          config,
		source_gid:      source_gid,
	}
}

func (kv *ShardKV) PersistNetDaemon() {
	Lock(kv, lock_trace, "PersistNetDaemon")
	defer Unlock(kv, lock_trace, "PersistNetDaemon")
	Assert(kv.state.LatestConfig.Num > 0, "Uninitialized config")
	for {
		if kv.IsLeader() && !kv.state.AreShardsStable() {
			to_transfer := map[int][]Shard{}
			for shard_num, gid := range kv.state.LatestConfig.Shards {
				Assert(kv.state.Shards[shard_num] != nil, "Nil shard %d %v", shard_num, kv.state.Shards)
				if gid != kv.gid && kv.state.Shards[shard_num].Status == PUSHING {
					to_transfer[gid] = append(to_transfer[gid], *kv.state.Shards[shard_num])
				}
			}
			if len(to_transfer) > 0 {
				Debug(dInfo, "G%d retry transfer shard %v", kv.gid, to_transfer)
				NewSendTransferShard(to_transfer, *kv.state.LatestConfig, kv.gid).SendNetworkMessage(kv)
			}
		}
		UnlockAndSleepFor(kv, raft.GetSendTime()*10)
	}
}
