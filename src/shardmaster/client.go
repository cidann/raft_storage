package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"dsys/labrpc"
	"dsys/raft"
	"dsys/raft_helper"
	"math/big"
	"sync"
	"time"
)

var id_counter int = 0
var id_counter_lock = sync.Mutex{}

func get_id() int {
	id_counter_lock.Lock()
	defer id_counter_lock.Unlock()
	id := id_counter
	id_counter += 1

	return id
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int
	serial  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers: servers,
		id:      get_id(),
		serial:  0,
	}
}

func (ck *Clerk) Query(num int) Config {
	defer func() {
		ck.serial++
	}()
	args := QueryArgs{
		Num: num,
		Op:  raft_helper.NewOpBase(ck.serial, ck.id, QUERY),
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			if raft_helper.Send_for(srv, "ShardMaster.Query", &args, &reply, raft.GetSendTime()*30) == raft_helper.VALID {
				return reply.Config
			}
		}
		Debug(dError, "C%d failed Query serial: %d", ck.id, ck.serial)
		time.Sleep(raft.GetMaxElectionTime())
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	defer func() {
		ck.serial++
	}()
	args := JoinArgs{
		Servers: servers,
		Op:      raft_helper.NewOpBase(ck.serial, ck.id, JOIN),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			if raft_helper.Send_for(srv, "ShardMaster.Join", &args, &reply, raft.GetSendTime()*20) == raft_helper.VALID {
				return
			}
		}
		Debug(dError, "C%d failed Join serial: %d", ck.id, ck.serial)
		time.Sleep(raft.GetMaxElectionTime())
	}
}

func (ck *Clerk) Leave(gids []int) {
	defer func() {
		ck.serial++
	}()
	args := LeaveArgs{
		GIDs: gids,
		Op:   raft_helper.NewOpBase(ck.serial, ck.id, LEAVE),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			if raft_helper.Send_for(srv, "ShardMaster.Leave", &args, &reply, raft.GetSendTime()*20) == raft_helper.VALID {
				return
			}
		}
		Debug(dError, "C%d failed Leave serial: %d", ck.id, ck.serial)
		time.Sleep(raft.GetMaxElectionTime())
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	defer func() {
		ck.serial++
	}()
	args := MoveArgs{
		Shard: shard,
		GID:   gid,
		Op:    raft_helper.NewOpBase(ck.serial, ck.id, MOVE),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			if raft_helper.Send_for(srv, "ShardMaster.Move", &args, &reply, raft.GetSendTime()*20) == raft_helper.VALID {
				return
			}
		}
		Debug(dError, "C%d failed Move serial: %d", ck.id, ck.serial)
		time.Sleep(raft.GetMaxElectionTime())
	}
}
