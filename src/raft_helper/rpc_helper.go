package raft_helper

import (
	"dsys/labrpc"
	"dsys/sync_helper"
	"time"
)

func Is_valid(reply Reply) bool {
	return reply.Get_success() && !reply.Get_outDated()
}

func Send_for(server *labrpc.ClientEnd, rpc_name string, args Op, reply Reply, timeout time.Duration) bool {
	result_chan := sync_helper.GetChanForFunc[bool](func() { server.Call(rpc_name, args, reply) })
	timeout_chan := sync_helper.GetChanForTime[bool](timeout)
	var res bool = false

	select {
	case <-result_chan:
		if Is_valid(reply) {
			res = true
		}
	case <-timeout_chan:
		Debug(dError, "C%d timeout %s serial: %d", args.Get_sid(), rpc_name, args.Get_serial())
	}

	return res
}
