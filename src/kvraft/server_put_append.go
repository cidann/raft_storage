package kvraft

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Lock(kv, lock_trace, "PutAppend")
	defer Unlock(kv, lock_trace, "PutAppend")

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	Debug(dClient, "S%d <- C%d Received PutAppend Serial:%d Key:%s as Leader", kv.me, args.Sid, args.Serial, args.Key)

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
	Debug(dClient, "S%d <- C%d PutAppend Serial:%d Key:%s done", kv.me, args.Sid, args.Serial, args.Key)

}
