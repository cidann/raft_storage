package kvraft

import "dsys/raft"

func (kv *KVServer) ApplyDaemon() {
	Lock(kv, lock_trace, "ApplyDaemon")
	defer Unlock(kv, lock_trace, "ApplyDaemon")

	for !kv.killed() {
		msg := UnlockUntilChanReceive(kv, kv.applyCh)
		switch msg.Command.(type) {
		case Op:
			kv.handleOperation(&msg)
		case SnapshotData:
			kv.handleSnapshot(&msg)
		}
	}
}

func (kv *KVServer) handleOperation(msg *raft.ApplyMsg) {
	DPrintf("[%d] handleOperation", kv.me)
	operation := msg.Command.(Op)
	op_result := ""
	if !kv.tracker.AlreadyProcessed(&operation) || operation.Type == GET {
		op_result = kv.state.Apply(operation, msg.Index(), msg.CommandTerm, kv.me)
		//DPrintf("[%d] state after entry %s", kv.me, kv.state.KvState)
	}
	kv.tracker.ProcessRequest(&operation, op_result)

	if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
		kv.rf.ApplicationSnapshot(kv.CreateSnapshot(), kv.state.LastIndex, kv.state.LastTerm)
		DPrintf("[%d] snapshoted at [index: %d, term: %d]", kv.me, kv.state.LastIndex, kv.state.LastTerm)
	}
}

func (kv *KVServer) handleSnapshot(msg *raft.ApplyMsg) {
	DPrintf("[%d] handleSnapshot", kv.me)
	snapshot := msg.Command.(SnapshotData)
	for i := range kv.tracker.request_chan {
		kv.tracker.DiscardRequestFrom(i)
	}
	kv.LoadSnapshot(snapshot.Data)
}
