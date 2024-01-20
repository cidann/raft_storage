package shardmaster

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

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Sid     int
	Serial  int
}

type JoinReply struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}

type LeaveArgs struct {
	GIDs   []int
	Sid    int
	Serial int
}

type LeaveReply struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}

type MoveArgs struct {
	Shard  int
	GID    int
	Sid    int
	Serial int
}

type MoveReply struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}

type QueryArgs struct {
	Num    int // desired config number
	Sid    int
	Serial int
}

type QueryReply struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
	Config     Config
}
