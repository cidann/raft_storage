package shardkv

import (
	"dsys/raft"
	"dsys/raft_helper"
	"dsys/shardmaster"
	"sync/atomic"
)

var true_serial_new_config int64 = 0

func (kv *ShardKV) NewConfig(args *NewConfigArgs, reply *NewConfigReply) {
	cur_serial := atomic.AddInt64(&true_serial_new_config, 1)

	Debug(dClient, "G%d <- C%d Received New Config Serial:%d as Leader true#%d", kv.gid, args.Get_sid(), args.Get_serial(), cur_serial)
	raft_helper.HandleStateChangeRPC(kv, "NewConfig", args, reply)

	Debug(dClient, "G%d <- C%d New Config Serial:%d done true#%d Outdated:%t", kv.gid, args.Get_sid(), args.Get_serial(), cur_serial, reply.Get_outDated())

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
	Assert(kv.state.LatestConfig.Num > 0, "Uninitialized config")

	for !kv.killed() {
		if _, isLeader := kv.GetLeader(); isLeader {
			cur_config_num := kv.state.LatestConfig.Num
			var config shardmaster.Config
			UnlockUntilFunc(kv, func() {
				config = kv.clerk_pool.AsyncQuery(cur_config_num + 1)
			})

			Assert(kv.state.LatestConfig.Num == cur_config_num, "ConfigPullDaemon is the only way to change config")
			Debug(dTrace, "G%d Queried new config %v cur config %v stable %v", kv.gid, config, kv.state.LatestConfig, kv.state.AreShardsStable())
			if config.Num == kv.state.LatestConfig.Num+1 && kv.state.AreShardsStable() {
				Debug(dTrace, "G%d start sending config %v", kv.gid, config)
				UnlockUntilFunc(kv, func() {
					kv.ReplicateConfig(&config)
				})
				Debug(dTrace, "G%d done sending config %v", kv.gid, config)
			}
			/*
				go func() {
					config := kv.mck.Query(kv.state.LatestConfig.Num + 1)
					if config.Num == kv.state.LatestConfig.Num+1 && kv.state.AreShardsStable() {
						go kv.clerk_pool.AsyncNewConfig(kv.gid, config)
					}
				}()
			*/
		}
		UnlockAndSleepFor(kv, raft.GetSendTime())
	}
}

func (kv *ShardKV) InitConfig() {
	Lock(kv, lock_trace, "ConfigPullDaemon")
	defer Unlock(kv, lock_trace, "ConfigPullDaemon")
	var config shardmaster.Config
	for !(config.Num > 0) {
		config = kv.mck.Query(1)
		if !(config.Num > 0) {
			UnlockAndSleepFor(kv, raft.GetSendTime())
		}
	}
	Assert(config.Num == 1, "Expect config num to be 1")
	kv.state.InitConfig(&config)
	Debug(dInit, "S%d G%d status %v init config %v", kv.me, kv.gid, kv.state.GetShardsStatus(), kv.state.LatestConfig)
}
