package shardkv

import (
	"dsys/shardmaster"
)

const (
	QUERYING ConfigCommitPhase = iota
	VOTING_COMMIT
	//VOTING_ABORT likely don't need
	DECIDING_COMMIT
	DECIDING_ABORT
	ACKNOWLEDGING
	NONE
)

type ConfigManager struct {
	Phase           ConfigCommitPhase
	Commited_config shardmaster.Config
	Staged_config   shardmaster.Config
	Coordinator     int
	//coordinator states
	Participants           []int
	Commiting_participants map[int]bool
	Ack_participants       map[int]bool
}

func NewConfigManager() *ConfigManager {
	phase := NONE

	return &ConfigManager{
		Phase:                  phase,
		Commiting_participants: map[int]bool{},
		Ack_participants:       map[int]bool{},
	}
}

func (ss *ServerState) HandleNewConfig(operation *NewConfigArgs) {
	if ss.Config_manager.Staged_config.Num < operation.Config.Num && ss.Config_manager.can_restart() {

	}
}

func (ss *ServerState) HandlePrepareConfig(operation *PrepareConfigArgs) {
	if !ss.Config_manager.is_coordinator() {
		if ss.Config_manager.Phase == NONE && ss.Config_manager.Commited_config.Num < operation.Config.Num {
			ss.Config_manager.Phase = VOTING_COMMIT
			ss.Config_manager.Coordinator = operation.Coordinator_gid
		} else if ss.Config_manager.Commited_config.Num == operation.Config.Num {
			//Ignore since its out of order message
		} else {
			panic("todo send abort since config change conflict")
		}
	} else {
		panic("todo send abort conflict change")
	}
}

func (ss *ServerState) HandleParticipantDecision(operation *ParticipantDecisionConfigArgs) {
	if ss.Config_manager.is_coordinator() {
		if ss.Config_manager.Phase == QUERYING && ss.Config_manager.Staged_config.Num == operation.Config.Num {
			if _, ok := ss.Config_manager.Commiting_participants[operation.Participant_gid]; !ok {
				if operation.Commit {
					ss.Config_manager.Commiting_participants[operation.Participant_gid] = true
					if len(ss.Config_manager.Commiting_participants) == len(ss.Config_manager.Participants) {
						ss.Config_manager.Phase = DECIDING_COMMIT
					}
				} else {
					ss.Config_manager.Phase = DECIDING_ABORT
				}
			} else {
				panic("todo repeat")
			}
		} else {
			panic("todo non matching message")
		}
	} else {
		panic("todo send abort conflict change")
	}
}

func (ss *ServerState) HandleCoordinatorDecisionConfig(operation *CoordinatorDecisionConfigArgs) {
	if !ss.Config_manager.is_coordinator() {
		if ss.Config_manager.Phase == VOTING_COMMIT && ss.Config_manager.Coordinator == operation.Coordinator_gid && ss.Config_manager.Staged_config.Num == operation.Config.Num {
			if operation.Commit {
				ss.Config_manager.Phase = ACKNOWLEDGING
				ss.Config_manager.Commited_config = operation.Config
			} else {
				ss.Config_manager.Phase = ACKNOWLEDGING
			}
		} else {
			panic("todo send abort since config change conflict or late msg")
		}
	} else {
		panic("todo send abort conflict change")
	}
}

func (ss *ServerState) HandleAckConfig(operation *AckConfigArgs) {
	if ss.Config_manager.is_coordinator() {
		if ss.Config_manager.Phase == DECIDING_ABORT || ss.Config_manager.Phase == DECIDING_COMMIT && ss.Config_manager.Staged_config.Num == operation.Config.Num {
			if ss.Config_manager.Phase == DECIDING_COMMIT {
				ss.Config_manager.Commited_config = operation.Config
			} else {

			}
		} else {
			panic("todo handle else")
		}
	} else {
		panic("todo send abort conflict change")
	}
}

func (ss *ServerState) HandleAckDone(operation *AckDoneArgs) {
	if !ss.Config_manager.is_coordinator() {
		if ss.Config_manager.Phase == ACKNOWLEDGING && ss.Config_manager.Commited_config.Num == operation.Config.Num {
			ss.Config_manager.Phase = NONE
		} else {
			panic("todo handle else")
		}
	} else {
		panic("todo send abort conflict change")
	}
}

func (manager *ConfigManager) 

func (manager *ConfigManager) can_restart() bool {
	restart := false
	switch manager.Phase {
	case QUERYING:
		restart = true
	case VOTING_COMMIT:
	//case VOTING_ABORT:
	case DECIDING_COMMIT:
	case DECIDING_ABORT:
	case ACKNOWLEDGING:
	case NONE:
		restart = true
	default:
		panic("unhandled case")
	}
	return restart
}

func (manager *ConfigManager) is_coordinator() bool {
	coordinator := false
	switch manager.Phase {
	case QUERYING:
		coordinator = true
	case VOTING_COMMIT:
	//case VOTING_ABORT:
	case DECIDING_COMMIT:
		coordinator = true
	case DECIDING_ABORT:
		coordinator = true
	case ACKNOWLEDGING:
	case NONE:
	default:
		panic("unhandled case")
	}
	return coordinator
}
