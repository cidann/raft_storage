package shardkv

import (
	"dsys/labgob"
	"dsys/raft_helper"
	"dsys/shardmaster"
)

type ShardStatus int

type Shard struct {
	ShardNum     int
	KvState      map[string]string
	ShardTracker RequestTracker
	Status       ShardStatus
}

type ServerState struct {
	Shards         map[int]*Shard
	GeneralTracker RequestTracker
	LastIndex      int
	LastTerm       int
	Gid            int
	LatestConfig   *shardmaster.Config
}

const (
	INVALID_SHARD_STATUS ShardStatus = iota
	OWN
	PULLING
	PUSHING
	DONT_OWN
)

func NewShard(num int, status ShardStatus) *Shard {
	return &Shard{
		ShardNum:     num,
		KvState:      map[string]string{},
		ShardTracker: *NewRequestTracker(),
		Status:       status,
	}
}

func (shard *Shard) Copy() *Shard {
	return &Shard{
		ShardNum:     shard.ShardNum,
		KvState:      CopyMap[string, string](shard.KvState),
		ShardTracker: *shard.ShardTracker.Copy(),
		Status:       shard.Status,
	}
}

func (shard *Shard) IsStable() bool {
	return shard.Status == OWN || shard.Status == DONT_OWN
}

func NewServerState(gid int) *ServerState {
	state := &ServerState{
		Shards:         make(map[int]*Shard),
		GeneralTracker: *NewRequestTracker(),
		LastIndex:      0,
		LastTerm:       0,
		Gid:            gid,
		LatestConfig:   &shardmaster.Config{},
	}

	return state
}

func (ss *ServerState) InitConfig(init_config *shardmaster.Config) {
	Assert(init_config.Num == 1, "Init config should start at num 1")
	for shard, owner_gid := range init_config.Shards {
		if owner_gid == ss.Gid {
			ss.Shards[shard] = NewShard(shard, OWN)
		} else {
			ss.Shards[shard] = NewShard(shard, DONT_OWN)
		}
	}
	ss.LatestConfig = init_config
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

func (ss *ServerState) DispatchOp(operation raft_helper.Op) {
	if ss.LatestConfig.Num == 0 {
		return
	}
	switch operation := operation.(type) {
	case *GetArgs:
		ss.ApplyKVState(operation)
	case *PutAppendArgs:
		ss.ApplyKVState(operation)
	case *NewConfigArgs:
		ss.HandleNewConfig(operation)
	case *TransferShardArgs:
		ss.HandleTransferShard(operation)
	case *ShardReceivedArgs:
		ss.HandleShardReceived(operation)
	default:
		panic("Unknown operation type")
	}
}

func (ss *ServerState) ApplyKVState(operation raft_helper.Op) {
	key, val := GetKeyVal(operation)
	shardNum := key2shard(key)
	if ss.HaveShard(shardNum) {
		result := ""
		if !ss.Shards[shardNum].ShardTracker.AlreadyProcessed(operation) || operation.Get_type() == GET {
			switch operation.Get_type() {
			case GET:
				result = ss.Get(shardNum, key)
			case PUT:
				ss.Put(shardNum, key, val)
			case APPEND:
				ss.Append(shardNum, key, val)
			}
		}
		ss.ProcessRequest(operation, result)
	} else {
		ss.Shards[shardNum].ShardTracker.DiscardRequestFrom(operation.Get_sid())
		Debug(dError, "G%d don't have shard %d config %v", ss.Gid, shardNum, ss.LatestConfig)
	}

}

func (ss *ServerState) HandleNewConfig(operation *NewConfigArgs) {
	Debug(dConf, "G%d already processed:%v new config %v", ss.Gid, ss.IsAlreadyProcessed(operation), operation.Config)
	if ss.IsAlreadyProcessed(operation) {
		ss.ProcessRequest(operation, "")
		return
	}

	if operation.Config.Num > ss.LatestConfig.Num {
		Assert(ss.AreShardsStable(), "Expected new config when cur config is stable")
		old_config := ss.LatestConfig
		transfer_shards := ss.installNewConfig(operation.Config)
		Debug(dTrace, "G%d old config %v new config %v transfer %v", ss.Gid, old_config, ss.LatestConfig, TransformMap(transfer_shards, func(val []Shard) []int {
			return Transform(val, func(val_shard Shard) int {
				return val_shard.ShardNum
			})
		}))
		// Just let daemon handle transfer to not overwhelm network and livestock
		//net_msg = NewSendTransferShard(transfer_shards, *ss.LatestConfig, ss.Gid)
	}
	ss.ProcessRequest(operation, struct{}{})

}

func (ss *ServerState) HandleTransferShard(operation *TransferShardArgs) {
	Debug(dTrans, "G%d already processed:%v new transfer shards %v", ss.Gid, ss.IsAlreadyProcessed(operation), operation.Shards)
	if ss.IsAlreadyProcessed(operation) {
		ss.ProcessRequest(operation, "")
		return
	}

	if operation.Config.Num == ss.LatestConfig.Num {
		for _, shard := range operation.Shards {
			if ss.Shards[shard.ShardNum].Status == OWN {
				continue
			}
			ss.Shards[shard.ShardNum] = shard.Copy()
			ss.Shards[shard.ShardNum].ShardTracker.request_chan = make(map[int]chan any)
			ss.Shards[shard.ShardNum].Status = OWN
			Debug(dTrace, "G%d installed shard %d kv %v", ss.Gid, shard.ShardNum, shard.KvState)
		}
	}
	//Can improve to hold onto the shards and when new config comes just install it and send decision
	//This would prevent the source group from resending
	if operation.Config.Num <= ss.LatestConfig.Num {
		ss.ProcessRequest(operation, struct{}{})
	}

}

func (ss *ServerState) HandleShardReceived(operation *ShardReceivedArgs) {
	Debug(dDECI, "G%d already processed:%v new decision from %d conf %v", ss.Gid, ss.IsAlreadyProcessed(operation), operation.Receiver_Gid, operation.Config)
	if ss.IsAlreadyProcessed(operation) {
		ss.ProcessRequest(operation, "")
		return
	}

	if operation.Config.Num == ss.LatestConfig.Num {
		ss.discardShardForGroup(operation.Receiver_Gid)
	}

	ss.ProcessRequest(operation, struct{}{})
}

func (ss *ServerState) installNewConfig(config shardmaster.Config) map[int][]Shard {
	Assert(ss.LatestConfig.Num < config.Num, "Installing a older config bug")
	to_transfer := map[int][]Shard{}

	for num, shard := range ss.Shards {
		Assert(shard.Status != PUSHING && shard.Status != PULLING, "Expected shards to be stable when changing config")
		gid := config.Shards[num]
		if gid != ss.Gid && shard.Status == OWN {
			shard.Status = PUSHING
			to_transfer[gid] = append(to_transfer[gid], *shard)
		} else if gid == ss.Gid && shard.Status != OWN {
			shard.Status = PULLING
		}
	}
	new_config := shardmaster.Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: CopyMap(config.Groups),
	}
	ss.LatestConfig = &new_config

	return to_transfer
}

/*
	func (ss *ServerState) transferShards(shards []Shard) {
		for _, shard := range shards {
			cur_shard, ok := ss.Shards[shard.ShardNum]
			if ok {
				if cur_shard.Config_num > shard.Config_num {
					panic("shard's applied config num should be <= overall config num")
				}
				if cur_shard.Status == OWN && cur_shard.Config_num != shard.Config_num {
					panic("Double ownership")
				}
			} else {
				ss.Shards[shard.ShardNum] = &shard
				cur_shard = &shard
			}
			cur_shard.Status = OWN
			cur_shard.Config_num = shard.Config_num
		}
	}
*/
func (ss *ServerState) discardShardForGroup(gid int) {

	for shard_num, shard_gid := range ss.LatestConfig.Shards {
		if shard_gid == gid {
			ss.Shards[shard_num].Status = DONT_OWN
			ss.Shards[shard_num].KvState = make(map[string]string)
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
		return false
	case *PutAppendArgs:
		shard_num := key2shard(op.Key)
		return ss.Shards[shard_num].ShardTracker.AlreadyProcessed(op)
	case *NewConfigArgs:
		return ss.GeneralTracker.AlreadyProcessed(op)
	case *TransferShardArgs:
		return ss.GeneralTracker.AlreadyProcessed(op)
	case *ShardReceivedArgs:
		return false
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
	case *ShardReceivedArgs:
		ss.GeneralTracker.ProcessRequest(op, op_result)
	default:
		panic("Other type not implemented")
	}
}

func (ss *ServerState) HaveShard(shardNum int) bool {
	shard, ok := ss.Shards[shardNum]
	return ok && shard.Status == OWN && ss.LatestConfig.Shards[shardNum] == ss.Gid
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

func (ss *ServerState) AreShardsStable() bool {
	for _, shard := range ss.Shards {
		if shard.Status != OWN && shard.Status != DONT_OWN {
			return false
		}
	}
	return true
}

func (ss *ServerState) GetShardsStatus() []ShardStatus {
	status := make([]ShardStatus, 10)
	for _, shard := range ss.Shards {
		Assert(shard != nil, "unitialized shard")
		status[shard.ShardNum] = shard.Status
	}
	return status
}

func (ss *ServerState) EncodeData(encoder labgob.LabEncoder) {
	encoder.Encode(*ss)
}

func (ss *ServerState) Recover() {
	for _, shard := range ss.Shards {
		shard.ShardTracker.request_chan = map[int]chan any{}
	}
	ss.GeneralTracker.request_chan = map[int]chan any{}
	ss.DiscardAllRequest()
}
