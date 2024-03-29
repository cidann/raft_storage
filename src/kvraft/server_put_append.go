package kvraft

import "sync/atomic"

var true_serial_put_append int64 = 0

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Lock(kv, lock_trace, "PutAppend")
	defer Unlock(kv, lock_trace, "PutAppend")
	cur_serial := atomic.AddInt64(&true_serial_put_append, 1)

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	Debug(dClient, "S%d <- C%d Received PutAppend Serial:%d Key/Val:{%s:%s} as Leader true#%d", kv.me, args.Sid, args.Serial, args.Key, args.Value, cur_serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   args.Type,
		Key:    args.Key,
		Value:  args.Value,
	}

	result_chan := make(chan string, 1)

	kv.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		kv.rf.Start(operation)
		var _, received = WaitUntilChanReceive(result_chan)
		reply.OutDated = !received
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "S%d <- C%d PutAppend Serial:%d Key:%s done true#%d Outdated:%t", kv.me, args.Sid, args.Serial, args.Key, cur_serial, reply.OutDated)

}
