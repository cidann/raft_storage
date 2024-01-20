package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"dsys/labrpc"
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
	args := &QueryArgs{
		Num:    num,
		Serial: ck.serial,
		Sid:    ck.id,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.Success && !reply.OutDated {
				return reply.Config
			}
		}
		//log.Println("Failed Query")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	defer func() {
		ck.serial++
	}()
	args := &JoinArgs{
		Servers: servers,
		Serial:  ck.serial,
		Sid:     ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.Success && !reply.OutDated {
				return
			}
		}
		//log.Println("Failed join")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	defer func() {
		ck.serial++
	}()
	args := &LeaveArgs{
		GIDs:   gids,
		Serial: ck.serial,
		Sid:    ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.Success && !reply.OutDated {
				return
			}
		}
		//log.Println("Failed Leave")
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	defer func() {
		ck.serial++
	}()
	args := &MoveArgs{
		Shard:  shard,
		GID:    gid,
		Serial: ck.serial,
		Sid:    ck.id,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.Success && !reply.OutDated {
				return
			}
		}
		//log.Println("Failed Move")
		time.Sleep(100 * time.Millisecond)
	}
}
