package kvraft

type ClientTracker struct {
	servers []bool
	curFlag bool
	cur     int
	hint    int
}

func NewClientTracker(server_num int) *ClientTracker {
	return &ClientTracker{
		servers: make([]bool, server_num),
		curFlag: false,
		cur:     0,
		hint:    -1,
	}
}

func (ct *ClientTracker) Next() (int, bool) {
	nxt_index := -1
	visited_all := false
	if ct.hint != -1 && ct.servers[ct.hint] == ct.curFlag {
		nxt_index = ct.hint
	} else {
		for ; ct.servers[ct.cur] != ct.curFlag; ct.cur = (ct.cur + 1) % len(ct.servers) {
			if ct.cur == len(ct.servers)-1 {
				ct.curFlag = !ct.curFlag
				visited_all = true
			}
		}
		nxt_index = ct.cur
	}

	return nxt_index, visited_all
}

func (ct *ClientTracker) RecordHint(nxt int) {
	ct.hint = nxt
}

func (ct *ClientTracker) RecordInvalid(server int) {
	defer func() { ct.servers[server] = !ct.servers[server] }()
	if server == ct.hint {
		ct.hint = -1
	}
}
