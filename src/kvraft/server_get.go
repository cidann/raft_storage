package kvraft

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	Lock(kv, lock_trace, "Get")
	defer Unlock(kv, lock_trace, "Get")

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	Debug(dClient, "C%d -> S%d Received Get Serial:%d as Leader", args.Sid, kv.me, args.Serial)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   GET,
		Key:    args.Key,
	}
	result_chan := make(chan string, 1)

	kv.tracker.RecordRequest(&operation, result_chan)
	start_and_wait := func() {
		kv.rf.Start(operation)
		reply.Value = WaitUntilChanReceive(result_chan)
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	Debug(dClient, "C%d -> S%d Get Serial:%d done", args.Sid, kv.me, args.Serial)

}
