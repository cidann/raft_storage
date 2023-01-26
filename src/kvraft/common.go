package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err bool

// Put or Append
type PutAppendArgs struct {
	Sender string
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Serial string
}

type PutAppendReply struct {
	Ok bool
}

type GetArgs struct {
	Sender string
	Key string
	// You'll have to add definitions here.
	Serial string
}

type GetReply struct {
	Ok   bool
	Value string
}
