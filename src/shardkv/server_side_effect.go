package shardkv

import "dsys/shardmaster"

type SideEffect interface {
	ApplySideEffect(kv *ShardKV)
}

type SendTransferDecision struct {
	config     shardmaster.Config
	source_gid int
	target_gid int
}

type SendTransferShards struct {
	config     shardmaster.Config
	gid_shards map[int][]Shard
	source_gid int
}

type NoSideEffect struct {
}

func (side_effect *SendTransferDecision) ApplySideEffect(kv *ShardKV) {
	if kv.IsLeader() {
		go kv.clerk_pool.AsyncTransferShardsDecision(side_effect.target_gid, side_effect.source_gid, side_effect.config)
	}
}

func (side_effect *SendTransferShards) ApplySideEffect(kv *ShardKV) {
	if kv.IsLeader() {
		for target_gid, shards := range side_effect.gid_shards {
			go kv.clerk_pool.AsyncTransferShards(target_gid, side_effect.source_gid, side_effect.config, shards)
		}
	}
}

func (side_effect *NoSideEffect) ApplySideEffect(kv *ShardKV) {

}

func NewNoSideEffect() *NoSideEffect {
	return &NoSideEffect{}
}

func NewSendTransferDecision(config shardmaster.Config, target_gid, source_gid int) *SendTransferDecision {
	return &SendTransferDecision{
		config:     config,
		target_gid: target_gid,
		source_gid: source_gid,
	}
}

func NewSendTransferShard(gid_shards map[int][]Shard, config shardmaster.Config, source_gid int) *SendTransferShards {
	return &SendTransferShards{
		gid_shards: gid_shards,
		config:     config,
		source_gid: source_gid,
	}
}
