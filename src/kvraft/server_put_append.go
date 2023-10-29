package kvraft

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Lock(kv, lock_trace, "PutAppend")
	defer Unlock(kv, lock_trace, "PutAppend")

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	DPrintf("[%d] Received PutAppend as leader", kv.me)

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
		WaitUntilChanReceive(result_chan)
		DPrintf("[%d to %d] from notification PutAPpend {%s:%s}", args.Sid, kv.me, args.Key, args.Value)
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	DPrintf("[%d to %d] PutAppend Request done", args.Sid, kv.me)

}
