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

func NewOpBase(serial, id int, op_type OperationType) OpBase {
	return OpBase{
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
