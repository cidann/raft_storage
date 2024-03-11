package shardkv

import (
	"dsys/labgob"
	"dsys/raft_helper"
	"dsys/shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Err int

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
)

const (
	COMMIT = iota
	ABORT
)

const (
	GET raft_helper.OperationType = iota
	PUT
	APPEND
	NEW_CONFIG
	TRANSFERSHARD
	TRANSFERSHARDDECISION
	NO_OP
)

// Put or Append
type PutAppendArgs struct {
	raft_helper.Op
	Key   string
	Value string
	Type  raft_helper.OperationType
}

type PutAppendReply struct {
	raft_helper.ReplyBase
	Err Err
}

type GetArgs struct {
	raft_helper.Op
	Key string
}

type GetReply struct {
	raft_helper.ReplyBase
	Err   Err
	Value string
}

type NewConfigArgs struct {
	raft_helper.Op
	Config shardmaster.Config
}

type NewConfigReply struct {
	raft_helper.ReplyBase
}

type TransferShardArgs struct {
	raft_helper.Op
	Config shardmaster.Config
	Shards []Shard
	Gid    int
}

type TransferShardReply struct {
	raft_helper.ReplyBase
}

type ShardReceivedArgs struct {
	raft_helper.Op
	Config       shardmaster.Config
	Receiver_Gid int
}

type ShardReceivedReply struct {
	raft_helper.ReplyBase
}

type NoOpArgs struct {
	raft_helper.Op
}
type NoOpReply struct {
	raft_helper.ReplyBase
}

func GetKeyVal(operation raft_helper.Op) (string, string) {
	var key, val string
	switch operation.Get_type() {
	case GET:
		key = operation.(*GetArgs).Key
	case PUT:
		key = operation.(*PutAppendArgs).Key
		val = operation.(*PutAppendArgs).Value
	case APPEND:
		key = operation.(*PutAppendArgs).Key
		val = operation.(*PutAppendArgs).Value
	}

	return key, val
}

func init() {
	labgob.Register(&raft_helper.OpBase{})
	labgob.Register(&raft_helper.ReplyBase{})
	labgob.Register(&GetArgs{})
	labgob.Register(&GetReply{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&PutAppendReply{})
	labgob.Register(&NewConfigArgs{})
	labgob.Register(&NewConfigReply{})
	labgob.Register(&TransferShardArgs{})
	labgob.Register(&TransferShardReply{})
	labgob.Register(&ShardReceivedArgs{})
	labgob.Register(&ShardReceivedReply{})
	labgob.Register(&NoOpArgs{})
	labgob.Register(&NoOpReply{})
}
