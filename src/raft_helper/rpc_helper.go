package raft_helper

import (
	"dsys/labrpc"
	"dsys/sync_helper"
	"time"
)

type ReplyBase struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}

type ClerkReply interface {
	Is_valid() bool
}

type ClerkReplyGet[T any] interface {
	get_result() T
	ClerkReply
}

type ClerkReplyPut interface {
	ClerkReply
}

func (reply *ReplyBase) Is_valid() bool {
	return reply.Success && !reply.OutDated
}

func Send_for(server *labrpc.ClientEnd, rpc_name string, args Op, reply ClerkReply, timeout time.Duration) bool {
	result_chan := sync_helper.GetChanForFunc[bool](func() { server.Call(rpc_name, args, reply) })
	timeout_chan := sync_helper.GetChanForTime[bool](timeout)
	var res bool = false

	select {
	case <-result_chan:
		if reply.Is_valid() {
			res = true
		}
	case <-timeout_chan:
		Debug(dError, "C%d timeout %s serial: %d", args.Get_sid(), rpc_name, args.Get_serial())
	}

	return res
}
