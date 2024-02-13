package shardkv

import "dsys/shardmaster"

type SideEffect interface {
	ApplySideEffect(kv *ShardKV)
}

type StartConfigProposal struct {
	config     shardmaster.Config
	source_gid int
}

type SendParticipantConfigDecision struct {
	commit       bool
	config_num   int
	owned_shards []int
	source_gid   int
	target_gid   int
}

type SendCoordinatorConfigDecision struct {
	commit             bool
	config             shardmaster.Config
	new_created_shards map[int][]int
	source_gid         int
}

type SendConfigAck struct {
	config_num int
	source_gid int
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

func NewStartConfigProposal(config shardmaster.Config, source_gid int) *StartConfigProposal {
	return &StartConfigProposal{
		config:     config,
		source_gid: source_gid,
	}
}

func NewSendParticipantConfigDecision(commit bool, config_num int, owned_shards []int, target_gid, source_gid int) *SendParticipantConfigDecision {
	return &SendParticipantConfigDecision{
		commit:       commit,
		config_num:   config_num,
		owned_shards: owned_shards,
		source_gid:   source_gid,
		target_gid:   target_gid,
	}
}

func NewSendCoordinatorConfigDecision(commit bool, config shardmaster.Config, new_created_shards map[int][]int, source_gid int) *SendCoordinatorConfigDecision {
	return &SendCoordinatorConfigDecision{
		commit:             commit,
		config:             config,
		new_created_shards: new_created_shards,
		source_gid:         source_gid,
	}
}

func NewSendConfigAck(config_num int, source_gid int) *SendConfigAck {
	return &SendConfigAck{
		config_num: config_num,
		source_gid: source_gid,
	}
}
