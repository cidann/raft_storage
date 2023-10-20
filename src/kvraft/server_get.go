package kvraft

import "sync/atomic"

var GetCount uint64 = 0

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	atomic.AddUint64(&GetCount, 1)
	Lock(kv, lock_trace, "Get")
	defer Unlock(kv, lock_trace, "Get")
	defer func() {
		atomic.StoreUint64(&GetCount, atomic.LoadUint64(&GetCount)-1)
		DPrintf("[%d] GetCount %d", kv.me, atomic.LoadUint64(&GetCount))
	}()

	if leader, isLeader := kv.GetLeader(); !isLeader {
		reply.Success = false
		reply.LeaderHint = leader
		return
	}
	DPrintf("[%d] Received Get as Leader", kv.me)

	operation := Op{
		Serial: args.Serial,
		Sid:    args.Sid,
		Type:   GET,
		Key:    args.Key,
	}
	result_chan := make(chan string)

	kv.tracker.RecordRequest(operation, result_chan)
	start_and_wait := func() {
		kv.rf.Start(operation)
		reply.Value = WaitUntilChanReceive(result_chan)
		DPrintf("[%d to %d] from notification Get {%s:%s}", args.Sid, kv.me, args.Key, reply.Value)
	}
	UnlockUntilChanReceive(kv, GetChanForFunc[any](start_and_wait))
	reply.Success = true
	DPrintf("[%d to %d] Get Request done", args.Sid, kv.me)

}
