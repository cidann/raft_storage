package raft_helper

import (
	"dsys/sync_helper"
	"time"
)

type StateMachine interface {
	sync_helper.Lockable
	GetLeader() (int, bool)
	GetId() int
	StartSetOp(Op, Reply) bool
	Initialized() bool
}

func HandleStateChangeRPC(machine StateMachine, name string, args Op, reply Reply) {
	if !machine.Initialized() {
		sync_helper.UnlockAndSleepFor(machine, time.Millisecond*100)
		return
	}
	if leader, isLeader := machine.GetLeader(); !isLeader {
		reply.Set_success(false)
		reply.Set_leaderHint(leader)
		return
	}

	reply.Set_success(machine.StartSetOp(args, reply))

}
