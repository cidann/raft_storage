package raft_helper

import (
	"dsys/sync_helper"
)

type StateMachine interface {
	sync_helper.Lockable
	GetLeader() (int, bool)
	GetId() int
	StartSetOp(Op, Reply) bool
}

func HandleStateChangeRPC(machine StateMachine, name string, args Op, reply Reply) {
	sync_helper.Lock(machine, lock_trace, name)
	defer sync_helper.Unlock(machine, lock_trace, name)

	if leader, isLeader := machine.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}

	reply.Set_success(machine.StartSetOp(args, reply))

}
