package kvraft

import "sync/atomic"

var true_serial_get int64 = 0

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	Lock(kv, lock_trace, "Get")
	defer Unlock(kv, lock_trace, "Get")
	cur_serial := atomic.AddInt64(&true_serial_get, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}

	Debug(dClient, "S%d <- C%d Received Get Serial:%d Key:%s as Leader true#%d", kv.me, args.Sid, args.Serial, args.Key, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   GET,
		Key:    args.Key,
	}
	result_chan := make(chan string, 1)

	kv.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		var received bool
		kv.rf.Start(operation)
		reply.Value, received = WaitUntilChanReceive(result_chan)
		if !received {
			reply.Value = "closed"
			reply.OutDated = true
		} else {
			reply.OutDated = false
		}
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d Get Serial:%d Key/Val:{%s:%s} done true#%d Outdated:%t", kv.me, args.Sid, args.Serial, args.Key, reply.Value, cur_serial, reply.OutDated)

}
