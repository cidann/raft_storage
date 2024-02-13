package shardkv

import (
	"dsys/raft"
	"dsys/raft_helper"
	"sync/atomic"
)

var true_serial_new_config int64 = 0
var true_serial_prepare_config int64 = 0
var true_serial_participant_decision_config int64 = 0
var true_serial_coordinator_decision_config int64 = 0
var true_serial_ack_config int64 = 0

func (kv *ShardKV) NewConfig(args *NewConfigArgs, reply *NewConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_new_config, 1)

	Debug(dClient, "S%d <- C%d Received New Config Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "NewConfig", args, reply)

	Debug(dClient, "S%d <- C%d New Config Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())

}

func (kv *ShardKV) PrepareConfig(args *PrepareConfigArgs, reply *PrepareConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_prepare_config, 1)

	Debug(dClient, "S%d <- C%d Received Prepare Config Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "PrepareConfig", args, reply)

	Debug(dClient, "S%d <- C%d Prepare Config Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) ParticipantDecisionConfig(args *ParticipantDecisionConfigArgs, reply *ParticipantDecisionConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_participant_decision_config, 1)

	Debug(dClient, "S%d <- C%d Received Participant Decision Config Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "ParticipantDecisionConfig", args, reply)

	Debug(dClient, "S%d <- C%d Participant Decision Config  Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) CoordinatorDecisionConfig(args *CoordinatorDecisionConfigArgs, reply *CoordinatorDecisionConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_coordinator_decision_config, 1)

	Debug(dClient, "S%d <- C%d Received Coordinator Decision Config Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "CoordinatorDecisionConfig", args, reply)

	Debug(dClient, "S%d <- C%d Coordinator Decision Config  Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) AckConfig(args *AckConfigArgs, reply *AckConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_ack_config, 1)

	Debug(dClient, "S%d <- C%d Received AckConfigConfig Serial:%d as Leader true#%d", kv.me, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "AckConfig", args, reply)

	Debug(dClient, "S%d <- C%d AckConfig Config  Serial:%d done true#%d Outdated:%t", kv.me, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())
}

func (kv *ShardKV) ConfigPullDaemon() {
	Lock(kv, lock_trace, "ConfigPullDaemon")
	defer Unlock(kv, lock_trace, "ConfigPullDaemon")

	for !kv.killed() {
		if _, isLeader := kv.GetLeader(); isLeader {
			config := kv.mck.Query(-1)
			go kv.clerk_pool.AsyncNewConfig(kv.gid, config)
		}
		UnlockAndSleepFor(kv, raft.GetSendTime()*10)
	}
}
