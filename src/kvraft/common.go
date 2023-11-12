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

//less up to date can close more up to date with this series of event
/*
request 0 sent
timeout
request 1 sent
server gets request 1 and gets channel to respond when done replicate/applying
server gets request 0 and closes old request chan and sets up new request chan
client who's current scope sent request 1 gets result from closed server(which is invalid)
*/
type PutAppendReply struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
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
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}
