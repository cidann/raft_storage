package shardkv

import (
	"dsys/raft"
	"dsys/raft_helper"
	"dsys/shardmaster"
	"sync/atomic"
	"time"
)

var true_serial_new_config int64 = 0

func (kv *ShardKV) NewConfig(args *NewConfigArgs, reply *NewConfigReply) {
	Lock(kv, lock_trace, "NewConfig")
	defer Unlock(kv, lock_trace, "NewConfig")
	cur_serial := atomic.AddInt64(&true_serial_new_config, 1)

	if !kv.Initialized() {
		UnlockAndSleepFor(kv, time.Millisecond*100)
		return
	}
	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}
	Debug(dClient, "G%d <- C%d Received New Config Serial:%d Status:%v true#%d", kv.gid, args.Get_sid(), args.Get_serial(), kv.state.GetShardsStatus(), cur_serial)

	reply.Set_success(kv.StartSetOp(args, reply))

	Debug(dClient, "G%d <- C%d Done New Config Serial:%d Status:%v true#%d Outdated:%t", kv.gid, args.Get_sid(), args.Get_serial(), kv.state.GetShardsStatus(), cur_serial, reply.Get_outDated())

}

func (kv *ShardKV) ReplicateConfig(config *shardmaster.Config) {
	clerk := kv.clerk_pool.GetClerk()
	defer kv.clerk_pool.PutClerk(clerk)

	clerk.serial += 1
	args := NewConfigArgs{
		Op:     raft_helper.NewOpBase(clerk.serial, clerk.id, NEW_CONFIG),
		Config: *config,
	}
	for kv.IsLeader() {
		reply := NewConfigReply{
			*raft_helper.NewReplyBase(),
		}
		kv.NewConfig(&args, &reply)
		if reply.Success && !reply.OutDated {
			return
		}
	}
}

func (kv *ShardKV) ConfigPullDaemon() {
	Lock(kv, lock_trace, "ConfigPullDaemon")
	defer Unlock(kv, lock_trace, "ConfigPullDaemon")
	Debug(dTrace, "G%d ConfigDaemon Start", kv.gid)

	//Daemon from before kill Assert(daemon_count == 1, "Ignore this failure this cannot happen from true server failure")
	Assert(kv.state.LatestConfig.Num > 0, "Uninitialized config")

	for !kv.killed() {
		if _, isLeader := kv.GetLeader(); isLeader {
			cur_config_num := kv.state.LatestConfig.Num
			var config shardmaster.Config
			UnlockPollingInterval(
				kv,
				func() {
					config = kv.clerk_pool.AsyncQuery(cur_config_num + 1)
				},
				func() bool {
					Debug(dTrace, "G%d Query interval incomplete", kv.gid)
					return true
				},
				raft.GetSendTime()*20,
			)

			//Old config can be in log and not applied due to server failure Assert(kv.state.LatestConfig.Num == cur_config_num, "ConfigPullDaemon is the only way to change config")
			Debug(dTrace, "G%d Queried new config %v cur config %v status %v", kv.gid, config, kv.state.LatestConfig, kv.state.GetShardsStatus())
			if config.Num == kv.state.LatestConfig.Num+1 && kv.state.AreShardsStable() {
				Debug(dTrace, "G%d start sending config %v", kv.gid, config)
				UnlockUntilFunc(kv, func() {
					kv.ReplicateConfig(&config)
				})
				Debug(dTrace, "G%d done sending config %v", kv.gid, config)
			}
		} else {
			Debug(dTrace, "G%dS%d did not query as its not leader", kv.gid, kv.me)
		}
		UnlockAndSleepFor(kv, raft.GetSendTime())
	}
	Debug(dTrace, "G%d ConfigDaemon Stopped", kv.gid)
}

func (kv *ShardKV) InitConfig() {
	Lock(kv, lock_trace, "ConfigPullDaemon")
	defer Unlock(kv, lock_trace, "ConfigPullDaemon")
	if kv.state.LatestConfig.Num >= 1 {
		return
	}

	var config shardmaster.Config
	for !(config.Num > 0) {
		Debug(dInit, "S%d G%d try init config", kv.me, kv.gid)
		config = kv.mck.Query(1)
		Debug(dInit, "S%d G%d init config success %v", kv.me, kv.gid, config)
		if !(config.Num > 0) {
			UnlockAndSleepFor(kv, raft.GetSendTime())
		}
	}
	Assert(config.Num == 1, "Expect config num to be 1")
	kv.state.InitConfig(&config)
	Debug(dInit, "S%d G%d status %v init config %v", kv.me, kv.gid, kv.state.GetShardsStatus(), kv.state.LatestConfig)
}
