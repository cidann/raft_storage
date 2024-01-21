package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"dsys/labrpc"
	"dsys/raft"
	"log"
	"math/big"
	"time"
)

var id_counter int = 0

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id     int
	serial int
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
	ck.serial = 0
	ck.id = id_counter

	id_counter++
	return ck
}

func (ck *Clerk) Query(num int) Config {
	defer func() {
		ck.serial++
	}()
	args := QueryArgs{
		Num:    num,
		Serial: ck.serial,
		Sid:    ck.id,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			if send_for(srv, "ShardMaster.Query", &args, &reply, raft.GetSendTime()) {
				return reply.Config
			}
		}
		log.Println("Failed Query")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	defer func() {
		ck.serial++
	}()
	args := JoinArgs{
		Servers: servers,
		Serial:  ck.serial,
		Sid:     ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			if send_for(srv, "ShardMaster.Join", &args, &reply, raft.GetSendTime()) {
				return
			}
		}
		log.Println("Failed join")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	defer func() {
		ck.serial++
	}()
	args := LeaveArgs{
		GIDs:   gids,
		Serial: ck.serial,
		Sid:    ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			if send_for(srv, "ShardMaster.Leave", &args, &reply, raft.GetSendTime()) {
				return
			}
		}
		log.Println("Failed Leave")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	defer func() {
		ck.serial++
	}()
	args := MoveArgs{
		Shard:  shard,
		GID:    gid,
		Serial: ck.serial,
		Sid:    ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			if send_for(srv, "ShardMaster.Move", &args, &reply, raft.GetSendTime()) {
				return
			}
		}
		log.Println("Failed Move")
		time.Sleep(100 * time.Millisecond)
	}
}

type ClerkReply interface {
	is_valid() bool
}

type ClerkReplyGet[T any] interface {
	get_result() T
	ClerkReply
}

type ClerkReplyPut interface {
	ClerkReply
}

func (reply *ReplyBase) is_valid() bool {
	return reply.Success && !reply.OutDated
}

func send_for(server *labrpc.ClientEnd, rpc_name string, args any, reply ClerkReply, timeout time.Duration) bool {
	result_chan := GetChanForFunc[bool](func() { server.Call(rpc_name, args, reply) })
	timeout_chan := GetChanForTime[bool](timeout)
	var res bool = false

	select {
	case <-result_chan:
		if reply.is_valid() {
			res = true
		}
	case <-timeout_chan:
	}

	return res
}
