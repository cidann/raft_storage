package transaction

type TransactionManager struct {
	Coordinator       map[TransactionIdPair]*TransactionCoordinator
	Participant       map[TransactionIdPair]*TransactionParticipant
	LastTransactionID int
	Gid               int
}

func NewTransactionManager(gid int) *TransactionManager {
	return &TransactionManager{
		Coordinator:       make(map[TransactionIdPair]*TransactionCoordinator),
		Participant:       make(map[TransactionIdPair]*TransactionParticipant),
		LastTransactionID: 0,
		Gid:               gid,
	}
}

func (tm *TransactionManager) GenerateTid() int {
	defer func() { tm.LastTransactionID += 1 }()
	return tm.LastTransactionID
}
