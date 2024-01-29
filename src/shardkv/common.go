package shardkv

import (
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
)

type TransactionIdPair struct {
	Gid int
	Tid int
}
type Transaction struct {
	raft_helper.Op
	TransactionIdPair
}

func (txn *Transaction) Get_id_pair() TransactionIdPair {
	return txn.TransactionIdPair
}

func NewTransactionIdPair(gid, tid int) *TransactionIdPair {
	return &TransactionIdPair{
		Gid: gid,
		Tid: tid,
	}
}

func NewTransaction(gid, tid int, op raft_helper.Op) *Transaction {
	return &Transaction{
		Op:                op,
		TransactionIdPair: *NewTransactionIdPair(gid, tid),
	}
}

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

type TransferShardDecisionArgs struct {
	raft_helper.Op
	Config shardmaster.Config
	Gid    int
}

type TransferShardDecisionReply struct {
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
