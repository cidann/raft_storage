package shardkv

import (
	"dsys/labrpc"
	"dsys/shardmaster"
	"sync"
)

type ClerkPool struct {
	mu       *sync.Mutex
	pool     []*Clerk
	masters  []*labrpc.ClientEnd
	make_end func(string) *labrpc.ClientEnd
}

func NewClerkPool(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ClerkPool {
	return &ClerkPool{
		mu:       &sync.Mutex{},
		pool:     []*Clerk{},
		masters:  masters,
		make_end: make_end,
	}
}

func (cpool *ClerkPool) GetClerk() *Clerk {
	var clerk *Clerk

	cpool.mu.Lock()
	defer cpool.mu.Unlock()

	if len(cpool.pool) == 0 {
		cpool.pool = append(cpool.pool, MakeClerk(cpool.masters, cpool.make_end))
	}

	clerk, cpool.pool = cpool.pool[0], cpool.pool[1:]

	return clerk
}

func (cpool *ClerkPool) PutClerk(clerk *Clerk) {
	cpool.mu.Lock()
	defer cpool.mu.Unlock()

	cpool.pool = append(cpool.pool, clerk)
}

func (cpool *ClerkPool) AsyncGet(key string) string {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	return clerk.Get(key)
}

func (cpool *ClerkPool) AsyncPut(key string, value string) {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	clerk.Put(key, value)
}

func (cpool *ClerkPool) AsyncAppend(key string, value string) {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	clerk.Append(key, value)
}

/*
func (cpool *ClerkPool) AsyncNewConfig(gid int, config shardmaster.Config) {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	clerk.NewConfig(gid, config)
}
*/

func (cpool *ClerkPool) AsyncTransferShards(target_gid, source_gid int, config shardmaster.Config, shards []Shard) {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	clerk.TransferShards(target_gid, source_gid, config, shards)
}

/*
func (cpool *ClerkPool) AsyncTransferShardsDecision(target_gid, source_gid int, config shardmaster.Config) {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	clerk.TransferShardsDecision(target_gid, source_gid, config)
}
*/

func (cpool *ClerkPool) AsyncQuery(config_num int) shardmaster.Config {
	clerk := cpool.GetClerk()
	defer cpool.PutClerk(clerk)
	return clerk.sm.Query(config_num)
}
