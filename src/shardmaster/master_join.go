package shardmaster

import (
	"dsys/labgob"
	"dsys/sync_helper"
	"sync/atomic"
)

type JoinOperationArgs struct {
	Server map[int][]string
}

func init() {
	labgob.Register(JoinOperationArgs{})
}

var true_serial_join int64 = 0

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sync_helper.Lock(sm, lock_trace, "Join")
	defer sync_helper.Unlock(sm, lock_trace, "Join")
	cur_serial := atomic.AddInt64(&true_serial_join, 1)

	if leader, isLeader := sm.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Join Serial:%d mapping:%v as Leader true#%d", sm.me, args.Sid, args.Serial, args.Servers, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   JOIN,
		Args:   JoinOperationArgs{Server: args.Servers},
	}
	result_chan := make(chan Config, 1)

	sm.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		sm.rf.Start(operation)
		var _, received = sync_helper.WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	sync_helper.UnlockUntilChanReceive(sm, sync_helper.GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Join Serial:%d mapping:%v done true#%d Outdated:%t", sm.me, args.Sid, args.Serial, args.Servers, cur_serial, reply.OutDated)
}
