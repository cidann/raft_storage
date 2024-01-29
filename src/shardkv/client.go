package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"dsys/labrpc"
	"dsys/raft"
	"dsys/raft_helper"
	"dsys/shardmaster"
)

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	id     int
	serial int
}

var id_counter int = 0

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Op:  raft_helper.NewOpBase(ck.serial, ck.id, GET),
		Key: key,
	}
	ck.serial += 1

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				var reply GetReply
				srv := ck.make_end(servers[si])
				if raft_helper.Send_for(srv, "ShardKV.Get", &args, &reply, raft.GetSendTime()) {
					return reply.Value
				} else if reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op_type raft_helper.OperationType) {
	args := PutAppendArgs{
		Op:    raft_helper.NewOpBase(ck.serial, ck.id, op_type),
		Key:   key,
		Value: value,
		Type:  op_type,
	}
	ck.serial += 1

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if raft_helper.Send_for(srv, "ShardKV.PutAppend", &args, &reply, raft.GetSendTime()) {
					return
				} else if reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) NewConfig(gid int, config shardmaster.Config) {
	args := NewConfigArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, NEW_CONFIG),
		Config: config,
	}
	ck.serial += 1

	for {
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply NewConfigReply
				if raft_helper.Send_for(srv, "ShardKV.NewConfig", &args, &reply, raft.GetSendTime()) {
					return
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		// this is mainly used to check the group still exist
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) TransferShards(target_gid, source_gid int, config shardmaster.Config, shards []Shard) {
	args := TransferShardArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, TRANSFERSHARD),
		Config: config,
		Shards: shards,
		Gid:    source_gid,
	}
	ck.serial += 1

	for {
		if servers, ok := ck.config.Groups[target_gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply TransferShardReply
				if raft_helper.Send_for(srv, "ShardKV.TransferShard", &args, &reply, raft.GetSendTime()) {
					return
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		// this is mainly used to check the group still exist
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) TransferShardsDecision(target_gid, source_gid int, config shardmaster.Config) {
	args := TransferShardDecisionArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, TRANSFERSHARDDECISION),
		Config: config,
		Gid:    source_gid,
	}
	ck.serial += 1

	for {
		if servers, ok := ck.config.Groups[target_gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply TransferShardDecisionReply
				if raft_helper.Send_for(srv, "ShardKV.TransferShard", &args, &reply, raft.GetSendTime()) {
					return
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		// this is mainly used to check the group still exist
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
