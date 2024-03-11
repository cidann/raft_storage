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
	"sync"
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
	id       int
	serial   int
}

var id_counter int = 0
var id_counter_lock = sync.Mutex{}

func get_id() int {
	id_counter_lock.Lock()
	defer id_counter_lock.Unlock()
	id := id_counter
	id_counter += 1
	return id
}

// which shard is a key in?
// please use this function,
// and please do not change it.
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

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	return &Clerk{
		sm:       shardmaster.MakeClerk(masters),
		config:   shardmaster.Config{},
		make_end: make_end,
		id:       get_id(),
		serial:   0,
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.serial += 1
	args := GetArgs{
		Op:  raft_helper.NewOpBase(ck.serial, ck.id, GET),
		Key: key,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				var reply GetReply
				srv := ck.make_end(servers[si])
				if raft_helper.Send_for(srv, "ShardKV.Get", &args, &reply, raft.GetSendTime()*30) == raft_helper.VALID {
					return reply.Value
				} else if reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				Debug(dError, "C%d failed get key/shard: %v/%v serial: %d config %v", ck.id, key, shard, ck.serial, ck.config)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op_type raft_helper.OperationType) {
	ck.serial += 1
	args := PutAppendArgs{
		Op:    raft_helper.NewOpBase(ck.serial, ck.id, op_type),
		Key:   key,
		Value: value,
		Type:  op_type,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if raft_helper.Send_for(srv, "ShardKV.PutAppend", &args, &reply, raft.GetSendTime()*30) == raft_helper.VALID {
					return
				} else if reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				Debug(dError, "C%d failed putappend key/shard: %v/%v serial: %d config %v", ck.id, key, shard, ck.serial, ck.config)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

/*Bugged because if future config does not have target group it will never finish
func (ck *Clerk) NewConfig(gid int, config shardmaster.Config) {
	ck.serial += 1
	args := NewConfigArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, NEW_CONFIG),
		Config: config,
	}

	for {
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				reply := NewConfigReply{
					*raft_helper.NewReplyBase(),
				}
				result := raft_helper.Send_for(srv, "ShardKV.NewConfig", &args, &reply, raft.GetSendTime()*30)
				if result == raft_helper.VALID {
					return
				}
				if result == raft_helper.TIMEOUT {
					continue
				}
				// ... not ok, or ErrWrongLeader
				Debug(dError, "C%d failed new config serial: %d success/outdated: %t/%t", ck.id, ck.serial, reply.Get_success(), reply.Get_outDated())
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		// this is mainly used to check the group still exist
		ck.config = ck.sm.Query(-1)
	}
}*/

func (ck *Clerk) TransferShards(target_gid, source_gid int, config shardmaster.Config, shards []Shard) {
	ck.serial += 1
	args := TransferShardArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, TRANSFERSHARD),
		Config: config,
		Shards: shards,
		Gid:    source_gid,
	}

	for {
		servers, ok := config.Groups[target_gid]
		Assert(ok, "Send shard to group %d not in config %v", target_gid, config)

		for si := 0; si < len(servers); si++ {
			srv := ck.make_end(servers[si])
			reply := TransferShardReply{
				*raft_helper.NewReplyBase(),
			}
			result := raft_helper.Send_for(srv, "ShardKV.TransferShard", &args, &reply, raft.GetSendTime()*30)
			if result == raft_helper.VALID {
				return
			}
			if result == raft_helper.TIMEOUT {
				continue
			}
			// ... not ok, or ErrWrongLeader
			Debug(dError, "C%d failed transfer shard serial: %d success/outdated: %t/%t", ck.id, ck.serial, reply.Get_success(), reply.Get_outDated())
		}

		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		// this is mainly used to check the group still exist
	}
}

/*Better performance is to just make group replicate the decision on its own when transfer succeed from clerk
func (ck *Clerk) TransferShardsDecision(target_gid, source_gid int, config shardmaster.Config) {
	ck.serial += 1
	args := ShardReceivedArgs{
		Op:     raft_helper.NewOpBase(ck.serial, ck.id, TRANSFERSHARDDECISION),
		Config: config,
		Gid:    source_gid,
	}

	servers, ok := ck.config.Groups[target_gid]
	if !ok {
		Assert(config.Num > 0, "Group not in current or previous config %v", config)
		old_config := ck.sm.Query(config.Num - 1)
		servers, ok = old_config.Groups[target_gid]
		Assert(ok, "Group not in current or previous config old %v new %v", old_config, config)
	}

	for {
		for si := 0; si < len(servers); si++ {
			srv := ck.make_end(servers[si])
			reply := ShardReceivedReply{
				*raft_helper.NewReplyBase(),
			}
			result := raft_helper.Send_for(srv, "ShardKV.TransferShardDecision", &args, &reply, raft.GetSendTime()*30)
			if result == raft_helper.VALID {
				return
			}
			if result == raft_helper.TIMEOUT {
				continue
			}
			// ... not ok, or ErrWrongLeader
			Debug(dError, "C%d failed transfer shard decision serial: %d success/outdated: %t/%t", ck.id, ck.serial, reply.Get_success(), reply.Get_outDated())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
*/

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
