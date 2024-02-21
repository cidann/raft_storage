package shardkv

import (
	"dsys/raft_helper"
)

type ConfigCommitPhase int

type Coordinator struct {
	State             *ServerState
	LastTransactionID int
	Gid               int
	Config_manager    *ConfigManager
}

func NewTransactionManager(gid int, state *ServerState) *Coordinator {
	return &Coordinator{
		LastTransactionID: 0,
		Gid:               gid,
		State:             state,
		Config_manager:    NewConfigManager(),
	}
}

func (c *Coordinator) GenerateTid() int {
	defer func() { c.LastTransactionID += 1 }()
	return c.LastTransactionID
}

func (c *Coordinator) DispatchOp(op raft_helper.Op) SideEffect {
	if !c.State.IsAlreadyProcessed(op) || op.Get_type() == GET {
		//Direct op
		switch operation := op.(type) {
		case *GetArgs:
			return c.State.ApplyKVState(operation)
		case *PutAppendArgs:
			return c.State.ApplyKVState(operation)
		}

		//Transaction like op
		switch operation := op.(type) {
		case *NewConfigArgs:
		case *PrepareConfigArgs:

		default:
			panic("Unhanlded type")
		}
	}

	return NewNoSideEffect()
}
