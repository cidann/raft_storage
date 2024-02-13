package shardmaster

import (
	"dsys/labgob"
	"dsys/raft_helper"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const (
	JOIN raft_helper.OperationType = iota
	LEAVE
	MOVE
	QUERY
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
//Invariant: 0 is invalid Gid
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	raft_helper.Op
}

type JoinReply struct {
	raft_helper.ReplyBase
}

type LeaveArgs struct {
	GIDs []int
	raft_helper.Op
}

type LeaveReply struct {
	raft_helper.ReplyBase
}

type MoveArgs struct {
	Shard int
	GID   int
	raft_helper.Op
}

type MoveReply struct {
	raft_helper.ReplyBase
}

type QueryArgs struct {
	Num int // desired config number
	raft_helper.Op
}

type QueryReply struct {
	raft_helper.ReplyBase
	Config Config
}

func init() {
	labgob.Register(&raft_helper.OpBase{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&JoinReply{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&LeaveReply{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&MoveReply{})
	labgob.Register(&QueryArgs{})
	labgob.Register(&QueryReply{})
}
