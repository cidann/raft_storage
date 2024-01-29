package shardkv

type TransactionRole int

const (
	COORDINATOR TransactionRole = iota
	PARTICIPANT
)

type TransactionStatus int

const (
	STAGING TransactionStatus = iota
)

type TransactionManager struct {
	ActiveTransactions map[TransactionIdPair]*TransactionState
	LastTransactionID  int
	Gid                int
}

type TransactionState struct {
	Role   TransactionRole
	Staged []*Transaction
}

func NewTransactionManager(gid int) *TransactionManager {
	return &TransactionManager{
		ActiveTransactions: make(map[TransactionIdPair]*TransactionState),
		LastTransactionID:  0,
		Gid:                gid,
	}
}

func (tm *TransactionManager) RecordTransaction(txn *Transaction) {
	tm.ActiveTransactions[txn.Get_id_pair()].Staged = append(tm.ActiveTransactions[txn.Get_id_pair()].Staged, txn)
}

func (tm *TransactionManager) GetStaged(txn_id_pair TransactionIdPair) []*Transaction {
	return tm.ActiveTransactions[txn_id_pair].Staged
}

func (tm *TransactionManager) GenerateTid() int {
	defer func() { tm.LastTransactionID += 1 }()
	return tm.LastTransactionID
}
