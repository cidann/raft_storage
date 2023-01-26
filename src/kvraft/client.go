package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../raft"
import (
	"time"
	"fmt"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
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
	// You'll have to add code here.
	DPrintf("NewNewNewNewNewNewNewNewNewNewNewNew")
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
	// You will have to modify this function.
	ck.serial+=1
	sender:=fmt.Sprintf("%p",ck)
	serial:=fmt.Sprintf("%d-%d",time.Now(),ck.serial)
	DPrintf("Client Get Request (%v) Serial[%v]======================================\n",key,serial)
	val:=""
	args:=&GetArgs{sender,key,serial}
	reply:=&GetReply{}
	ck.servers[ck.leader].Call("KVServer.Get", args, reply)
	for !reply.Ok{
		for i:=range ck.servers{
			args:=&GetArgs{key,serial}
			reply:=&GetReply{}
			ck.servers[i].Call("KVServer.Get", args, reply)
			
			if reply.Ok{
				ck.leader=i
				DPrintf("Client got Read (%v) Serial[%v]\n",key,serial)
				val=reply.Value
				return val
			}
		}
		time.Sleep(raft.GetMaxEletionTime())
		DPrintf("Retry%v\n","+++++++++++++++++++++++++")
	}
	val=reply.Value
	DPrintf("Client got Read (%v) Serial[%v]\n",key,serial)

	return val
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.serial+=1
	sender:=fmt.Sprintf("%p",ck)
	serial:=fmt.Sprintf("%d-%d",time.Now(),ck.serial)
	DPrintf("Client PutAppend Request [%v] (%v)/(%v) Serial[%v]]======================================\n",op,key,value,serial)
	args:=&PutAppendArgs{key,value,op,serial}
	reply:=&PutAppendReply{}
	ck.servers[ck.leader].Call("KVServer.PutAppend", args, reply)
	for !reply.Ok{
		for i:=range ck.servers{
			args:=&PutAppendArgs{sender,key,value,op,serial}
			reply:=&PutAppendReply{}
			ck.servers[i].Call("KVServer.PutAppend", args, reply)

			if reply.Ok{
				ck.leader=i
				DPrintf("Client got [%v] (%v)/(%v) Serial[%v]\n",op,key,value,serial)
				return
			}
		}
		time.Sleep(raft.GetMaxEletionTime())
	}
	DPrintf("Client got [%v] (%v)/(%v) Serial[%v]\n",op,key,value,serial)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
