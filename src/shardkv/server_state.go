package shardkv

import (
	"dsys/labgob"
	"dsys/raft_helper"
	"dsys/shardmaster"
)

type ShardOwnership int

type Shard struct {
	ShardNum     int
	KvState      map[string]string
	ShardTracker RequestTracker
	Ownership    ShardOwnership
}

type ServerState struct {
	Shards         map[int]*Shard
	GeneralTracker RequestTracker
	LastIndex      int
	LastTerm       int
	Gid            int
	Config_manager *ConfigManager
}

const (
	OWN ShardOwnership = iota
	DONT_OWN
)

func NewServerState(gid int) *ServerState {
	return &ServerState{
		Shards:         make(map[int]*Shard),
		GeneralTracker: *NewRequestTracker(),
		LastIndex:      0,
		LastTerm:       0,
		Gid:            gid,
		Config_manager: NewConfigManager(),
	}
}

func (ss *ServerState) Put(shard int, k, v string) {
	ss.Shards[shard].KvState[k] = v
}

func (ss *ServerState) Append(shard int, k, v string) {
	ss.Shards[shard].KvState[k] += v
}

func (ss *ServerState) Get(shard int, k string) string {
	return ss.Shards[shard].KvState[k]
}

func (ss *ServerState) DispatchOp(operation raft_helper.Op) SideEffect {
	switch operation := operation.(type) {
	case *GetArgs:
		return ss.ApplyKVState(operation)
	case *PutAppendArgs:
		return ss.ApplyKVState(operation)
	case *NewConfigArgs:
		return ss.HandleNewConfig(operation)
	case *PrepareConfigArgs:
		return ss.HandlePrepareConfig(operation)
	default:
		panic("Unknown operation type")
	}
}

func (ss *ServerState) ApplyKVState(operation raft_helper.Op) SideEffect {
	key, val := GetKeyVal(operation)
	shardNum := key2shard(key)
	if ss.HaveShard(shardNum) {
		result := ""
		switch operation.Get_type() {
		case GET:
			result = ss.Get(shardNum, key)
		case PUT:
			ss.Put(shardNum, key, val)
		case APPEND:
			ss.Append(shardNum, key, val)
		}
		ss.ProcessRequest(operation, result)
	} else {

	}
	return NewNoSideEffect()
}

func (ss *ServerState) installNewConfig(config shardmaster.Config) map[int][]Shard {
	to_transfer := map[int][]Shard{}

	for shardNum, shard := range ss.Shards {
		gid := config.Shards[shardNum]
		if gid != ss.Gid && shard.Ownership == OWN {
			to_transfer[gid] = append(to_transfer[gid], *shard)
			shard.Config_num = config.Num
			shard.Ownership = DONT_OWN
		}
	}

	return to_transfer
}

func (ss *ServerState) transferShards(shards []Shard) {
	for _, shard := range shards {
		cur_shard, ok := ss.Shards[shard.ShardNum]
		if ok {
			if cur_shard.Config_num > shard.Config_num {
				panic("shard's applied config num should be <= overall config num")
			}
			if cur_shard.Ownership == OWN && cur_shard.Config_num != shard.Config_num {
				panic("Double ownership")
			}
		} else {
			ss.Shards[shard.ShardNum] = &shard
			cur_shard = &shard
		}
		cur_shard.Ownership = OWN
		cur_shard.Config_num = shard.Config_num
	}
}

func (ss *ServerState) deleteShardsForGroup(gid int) {
	for shardNum, shard_gid := range ss.LatestConfig.Shards {
		if shard_gid == gid {
			shard, ok := ss.Shards[shardNum]
			if ok && shard.Ownership != DONT_OWN {
				panic("Trying to delete owned shard")
			}
			delete(ss.Shards, shardNum)
		}
	}

}

func (ss *ServerState) SetLatest(index, term int) {
	ss.LastIndex = index
	ss.LastTerm = term
}

func (ss *ServerState) DiscardAllRequest() {
	ss.GeneralTracker.DiscardAll()
	for _, shard := range ss.Shards {
		shard.ShardTracker.DiscardAll()
	}
}

func (ss *ServerState) RecordRequestGeneral(op raft_helper.Op, req_chan chan any) {
	ss.GeneralTracker.RecordRequest(op, req_chan)
}

func (ss *ServerState) RecordRequestShard(shard int, op raft_helper.Op, req_chan chan any) {
	ss.Shards[shard].ShardTracker.RecordRequest(op, req_chan)
}

func (ss *ServerState) IsAlreadyProcessed(op raft_helper.Op) bool {
	switch op := op.(type) {
	case *GetArgs:
		shard_num := key2shard(op.Key)
		return ss.Shards[shard_num].ShardTracker.AlreadyProcessed(op)
	case *PutAppendArgs:
		shard_num := key2shard(op.Key)
		return ss.Shards[shard_num].ShardTracker.AlreadyProcessed(op)
	case *NewConfigArgs:
		return ss.GeneralTracker.AlreadyProcessed(op)
	case *TransferShardArgs:
		return ss.GeneralTracker.AlreadyProcessed(op)
	case *TransferShardDecisionArgs:
		return ss.GeneralTracker.AlreadyProcessed(op)
	}
	panic("Other type not implemented")
}

func (ss *ServerState) ProcessRequest(op raft_helper.Op, op_result any) {
	switch op := op.(type) {
	case *GetArgs:
		shard_num := key2shard(op.Key)
		ss.Shards[shard_num].ShardTracker.ProcessRequest(op, op_result)
	case *PutAppendArgs:
		shard_num := key2shard(op.Key)
		ss.Shards[shard_num].ShardTracker.ProcessRequest(op, op_result)
	case *NewConfigArgs:
		ss.GeneralTracker.ProcessRequest(op, op_result)
	case *TransferShardArgs:
		ss.GeneralTracker.ProcessRequest(op, op_result)
	case *TransferShardDecisionArgs:
		ss.GeneralTracker.ProcessRequest(op, op_result)
	default:
		panic("Other type not implemented")
	}
}

func (ss *ServerState) HaveShard(shardNum int) bool {
	shard, ok := ss.Shards[shardNum]
	return ok && shard.Ownership == OWN && ss.LatestConfig.Shards[shardNum] == ss.Gid
}

func (ss *ServerState) GetOwnedShards() []int {
	owned_shard_nums := []int{}
	for shard_num, gid := range ss.LatestConfig.Shards {
		if ss.Gid == gid {
			owned_shard_nums = append(owned_shard_nums, shard_num)
		}
	}
	return owned_shard_nums
}

func (ss *ServerState) EncodeData(encoder labgob.LabEncoder) {
	encoder.Encode(*ss)
}
