package raft_helper

type OperationType int

type OpBase struct {
	Serial int
	Sid    int
	Type   OperationType
}

type Op interface {
	Get_serial() int
	Get_sid() int
	Get_type() OperationType
	Set_serial(int)
	Set_sid(int)
	Set_type(OperationType)
}

type ReplyBase struct {
	Success    bool
	LeaderHint int
	OutDated   bool //this is needed since a outdated request might close the channel of more up to date request
}

type Reply interface {
	Get_success() bool
	Get_leaderHint() int
	Get_outDated() bool
	Set_success(bool)
	Set_leaderHint(int)
	Set_outDated(bool)
}

func NewOpBase(serial, id int, op_type OperationType) *OpBase {
	return &OpBase{
		Serial: serial,
		Sid:    id,
		Type:   op_type,
	}
}

func (op *OpBase) Get_serial() int {
	return op.Serial
}

func (op *OpBase) Get_sid() int {
	return op.Sid
}

func (op *OpBase) Get_type() OperationType {
	return op.Type
}

func (op *OpBase) Set_serial(serial int) {
	op.Serial = serial
}

func (op *OpBase) Set_sid(sid int) {
	op.Sid = sid
}

func (op *OpBase) Set_type(opType OperationType) {
	op.Type = opType
}

func NewReplyBase() *ReplyBase {
	return &ReplyBase{}
}

func (r *ReplyBase) Get_success() bool {
	return r.Success
}

func (r *ReplyBase) Get_leaderHint() int {
	return r.LeaderHint
}

func (r *ReplyBase) Get_outDated() bool {
	return r.OutDated
}

func (r *ReplyBase) Set_success(success bool) {
	r.Success = success
}

func (r *ReplyBase) Set_leaderHint(leaderHint int) {
	r.LeaderHint = leaderHint
}

func (r *ReplyBase) Set_outDated(outDated bool) {
	r.OutDated = outDated
}
