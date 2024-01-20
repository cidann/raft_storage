package shardmaster

import (
	"dsys/labgob"
	"sync/atomic"
)

type QueryOperationArgs struct {
	Num int // desired config number
}

func init() {
	labgob.Register(QueryOperationArgs{})
}

var true_serial_query int64 = 0

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	Lock(sm, lock_trace, "Query")
	defer Unlock(sm, lock_trace, "Query")
	cur_serial := atomic.AddInt64(&true_serial_query, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Query Serial:%d Num: %v as Leader true#%d", sm.me, args.Sid, args.Serial, args.Num, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   QUERY,
		Args:   QueryOperationArgs{Num: args.Num},
	}
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var config, received = WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
		reply.Config = config
	}
	UnlockUntilChanReceive(sm, GetChanForFunc[any](start_and_wait))
	reply.Success = true

	Debug(dClient, "S%d <- C%d Query Serial:%d Num: %v Config:%+v done true#%d Outdated:%t", sm.me, args.Sid, args.Serial, args.Num, reply.Config, cur_serial, reply.OutDated)
}
