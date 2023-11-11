package kvraft

type ServerState struct {
	KvState   map[string]string
	LastIndex int
	LastTerm  int
}

func NewServerState() *ServerState {
	return &ServerState{
		KvState: make(map[string]string),
	}
}

func (ss *ServerState) Put(k, v string) {
	ss.KvState[k] = v
}

func (ss *ServerState) Append(k, v string) {
	ss.KvState[k] += v
}

func (ss *ServerState) Get(k string) string {
	return ss.KvState[k]
}

func (ss *ServerState) Apply(operation Op, server int) string {
	switch operation.Type {
	case GET:
		return ss.Get(operation.Key)
	case PUT:
		ss.Put(operation.Key, operation.Value)
	case APPEND:
		ss.Append(operation.Key, operation.Value)
	}
	return ""
}

func (ss *ServerState) SetLatest(index, term int) {
	ss.LastIndex = index
	ss.LastTerm = term
}
