package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Type   OperationType
	Serial int
	Sid    int
}

type PutAppendReply struct {
	Success    bool
	LeaderHint int
}

type GetArgs struct {
	Key    string
	Serial int
	Sid    int
}

type GetReply struct {
	Success    bool
	Value      string
	LeaderHint int
}
