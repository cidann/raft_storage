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

func (ss *ServerState) Apply(operation Op, index, term, server int) string {
	defer func() {
		ss.LastIndex = index
		ss.LastTerm = term
	}()
	switch operation.Type {
	case GET:
		DPrintf("[%d] Apply Replicated Get {%s}", server, operation.Key)
		return ss.Get(operation.Key)
	case PUT:
		DPrintf("[%d] Apply Replicated PUT {%s:%s}", server, operation.Key, operation.Value)
		ss.Put(operation.Key, operation.Value)
	case APPEND:
		DPrintf("[%d] Apply Replicated APPEND {%s:%s}", server, operation.Key, operation.Value)
		ss.Append(operation.Key, operation.Value)
	}
	return ""
}
