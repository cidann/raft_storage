package kvraft

import (
	"crypto/rand"
	"dsys/labrpc"
	"dsys/raft"
	"math/big"
	"time"
)

var id_counter int = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int
	serial  int
	tracker *ClientTracker
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = id_counter
	ck.serial = 0
	ck.tracker = NewClientTracker(len(servers), 2, id_counter)

	id_counter++
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	for {
		args := GetArgs{
			Key:    key,
			Serial: ck.serial,
			Sid:    ck.id,
		}
		reply := GetReply{
			Success:    false,
			Value:      "",
			LeaderHint: -1,
		}

		target_server, visited_all := ck.tracker.Next()
		Debug(dClient, "S%d <- C%d try to Get Serial: %d", target_server, ck.id, args.Serial)
		result_chan := GetChanForFunc[bool](func() { ck.servers[target_server].Call("KVServer.Get", &args, &reply) })
		timeout_chan := GetChanForTime[bool](raft.GetSendTime())
		select {
		case <-result_chan:
			if reply.Success {
				Debug(dClient, "S%d <- C%d successfull Get Serial: %d", target_server, ck.id, args.Serial)
				ck.serial++
				return reply.Value
			} else {
				Debug(dClient, "S%d <- C%d failed Get Serial: %d", target_server, ck.id, args.Serial)
				ck.tracker.RecordInvalid(target_server)
				if reply.LeaderHint != -1 {
					ck.tracker.RecordHint(reply.LeaderHint)
				}
			}
		case <-timeout_chan:
			Debug(dClient, "S%d <- C%d timed out Get Serial: %d", target_server, ck.id, args.Serial)
			ck.tracker.RecordInvalid(target_server)
		}
		if visited_all {
			time.Sleep(raft.GetMaxElectionTime())
		}
	}
}

/*
Remeber to use hint on who is leader later
*/
func (ck *Clerk) PutAppend(key string, value string, op OperationType) {
	for {
		args := PutAppendArgs{
			Key:    key,
			Value:  value,
			Type:   op,
			Serial: ck.serial,
			Sid:    ck.id,
		}
		reply := PutAppendReply{
			Success:    false,
			LeaderHint: -1,
		}
		target_server, visited_all := ck.tracker.Next()
		Debug(dClient, "S%d <- C%d try to PutAppend Serial: %d", target_server, ck.id, args.Serial)
		result_chan := GetChanForFunc[bool](func() { ck.servers[target_server].Call("KVServer.PutAppend", &args, &reply) })
		timeout_chan := GetChanForTime[bool](raft.GetSendTime())
		select {
		case <-result_chan:
			if reply.Success {
				Debug(dClient, "S%d <- C%d successfull PutAppend Serial: %d", target_server, ck.id, args.Serial)
				ck.serial++
				return
			} else {
				Debug(dClient, "S%d <- C%d failed PutAppend Serial: %d", target_server, ck.id, args.Serial)
				ck.tracker.RecordInvalid(target_server)
				if reply.LeaderHint != -1 {
					ck.tracker.RecordHint(reply.LeaderHint)
				}
			}
		case <-timeout_chan:
			Debug(dClient, "S%d <- C%d timed out PutAppend Serial: %d", target_server, ck.id, args.Serial)
			ck.tracker.RecordInvalid(target_server)
		}
		if visited_all {
			time.Sleep(raft.GetMaxElectionTime())
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
