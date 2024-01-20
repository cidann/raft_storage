package shardmaster

import (
	"dsys/labgob"
	"sync/atomic"
)

type LeaveOperationArgs struct {
	GIDs []int
}

func init() {
	labgob.Register(LeaveOperationArgs{})
}

var true_serial_leave int64 = 0

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	Lock(sm, lock_trace, "Leave")
	defer Unlock(sm, lock_trace, "Leave")
	cur_serial := atomic.AddInt64(&true_serial_leave, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Leave Serial:%d GIDs:%v as Leader true#%d", sm.me, args.Sid, args.Serial, args.GIDs, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   LEAVE,
		Args:   LeaveOperationArgs{GIDs: args.GIDs},
	}
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var _, received = WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	UnlockUntilChanReceive(sm, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Leave Serial:%d GIDs:%v done true#%d Outdated:%t", sm.me, args.Sid, args.Serial, args.GIDs, cur_serial, reply.OutDated)

}
