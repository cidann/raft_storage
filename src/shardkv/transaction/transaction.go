package transaction

import "dsys/raft_helper"

type TransactionRole int

const (
	COORDINATOR TransactionRole = iota
	PARTICIPANT
)

type CoordinatorStatus int
type ParticipantStatus int

const (
	COORDINATORSTAGING CoordinatorStatus = iota
	QUERYING
	COORDINATORCOMMITING
	COORDINATORABORTING
)

const (
	PARTICIPANTSTAGING ParticipantStatus = iota
	PARTICIPANTCOMMITWAIT
	PARTICIPANTABORTING
	ACKNOWLEDGING
)

type TransactionIdPair struct {
	Gid int
	Tid int
}

func NewTransactionIdPair(gid, tid int) *TransactionIdPair {
	return &TransactionIdPair{
		Gid: gid,
		Tid: tid,
	}
}

type TransactionCoordinator struct {
	TransactionIdPair
	Status             CoordinatorStatus
	Participant_status map[int]ParticipantStatus
}

type TransactionParticipant struct {
	TransactionIdPair
	Status ParticipantStatus
	Staged []raft_helper.Op
}

func NewTransactionCoordinator(gid, tid int, participant_gid []int) *TransactionCoordinator {
	participant_status := map[int]ParticipantStatus{}
	for _, p_gid := range participant_gid {
		participant_status[p_gid] = PARTICIPANTSTAGING
	}

	return &TransactionCoordinator{
		TransactionIdPair:  *NewTransactionIdPair(gid, tid),
		Status:             COORDINATORSTAGING,
		Participant_status: participant_status,
	}
}

func NewtransactionParticipant(gid, tid int) *TransactionParticipant {
	return &TransactionParticipant{
		TransactionIdPair: *NewTransactionIdPair(gid, tid),
		Status:            PARTICIPANTSTAGING,
		Staged:            []raft_helper.Op{},
	}
}

func (txn *TransactionCoordinator) Get_id_pair() TransactionIdPair {
	return txn.TransactionIdPair
}

func (txn *TransactionCoordinator) Start_commit() {
	if txn.Status != COORDINATORSTAGING && txn.Status != QUERYING {
		panic("Already started commiting")
	}
	txn.Status = QUERYING
}

func (txn *TransactionCoordinator) Record_vote(participant_gid int, commit bool) {
	if txn.Status != QUERYING {
		panic("Received vote for invalid stage of commit(might remove panic later)")
	}
	if commit {
		txn.Participant_status[participant_gid] = PARTICIPANTCOMMITWAIT
	} else {
		txn.Participant_status[participant_gid] = PARTICIPANTABORTING
	}
}

func (txn *TransactionCoordinator) Record_ack(participant_gid int) {
	if txn.Status != COORDINATORCOMMITING && txn.Status != COORDINATORABORTING {
		panic("Received ack for invalid stage of commit(might remove panic later)")
	}
	txn.Participant_status[participant_gid] = ACKNOWLEDGING
}

func (txn *TransactionParticipant) Get_id_pair() TransactionIdPair {
	return txn.TransactionIdPair
}

func (txn *TransactionParticipant) Add_op(op raft_helper.Op) {
	txn.Staged = append(txn.Staged, op)
}

func (txn *TransactionParticipant) Record_start_commit() {

}
