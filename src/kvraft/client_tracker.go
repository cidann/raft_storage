package kvraft

type ClientTracker struct {
	servers      []bool
	curFlag      bool
	cur          int
	hint         int
	server_retry []int
	retry        int
	owner_id     int
}

func NewClientTracker(server_num int, retry int, owner_id int) *ClientTracker {
	return &ClientTracker{
		servers:      make([]bool, server_num),
		curFlag:      false,
		cur:          0,
		hint:         -1,
		server_retry: make([]int, server_num),
		retry:        retry,
		owner_id:     owner_id,
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
	if ct.server_retry[server] > ct.retry {
		panic("Should retried not be greater max retry")
	}
	if ct.server_retry[server] == ct.retry {
		if server == ct.hint {
			ct.hint = -1
		}
		ct.servers[server] = !ct.servers[server]
		ct.server_retry[server] = 0
	} else {
		ct.server_retry[server]++
	}
}
