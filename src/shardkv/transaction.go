package shardkv

import "dsys/labgob"

type TransactionIdPair struct {
	Gid int
	Tid int
}

type TransactionMessage interface {
	GetId() TransactionIdPair
}

func NewTransactionIdPair(gid, tid int) *TransactionIdPair {
	return &TransactionIdPair{
		Gid: gid,
		Tid: tid,
	}
}

func init() {
	labgob.Register(&ParticipantMessage{})
	labgob.Register(&CoordinatorMessage{})
}
