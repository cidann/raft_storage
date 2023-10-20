package kvraft

import "dsys/raft"

func (kv *KVServer) ApplyDaemon() {
	Lock(kv, lock_trace, "ApplyDaemon")
	defer Unlock(kv, lock_trace, "ApplyDaemon")

	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			operation := msg.Command.(Op)
			op_result := ""
			if !kv.tracker.AlreadyProcessed(operation) || operation.Type == GET {
				op_result = kv.state.Apply(operation, kv.me)
				DPrintf("[%d] state after entry %s", kv.me, kv.state.kvState)
			}
			kv.tracker.ProcessRequest(operation, op_result)

		default:
			DPrintf("[%d] Sleep since there is not replicated entries", kv.me)
			UnlockAndSleepFor(kv, raft.GetSendTime())
		}
	}
}
