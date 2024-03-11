package shardmaster

import (
	"fmt"
	"math"
)

type ConfigState struct {
	Configs   []Config // indexed by config num
	LastIndex int
	LastTerm  int
}

type GroupShardCount struct {
	gid   int
	count int
}

func NewConfigState() *ConfigState {
	new_config := ConfigState{}
	new_config.Configs = make([]Config, 1)
	new_config.Configs[0].Groups = map[int][]string{}
	return &new_config
}

func (cs *ConfigState) Join(mapping map[int][]string) {
	latest_config := cs.LatestConfig()
	new_config := Config{Num: latest_config.Num + 1, Groups: CopyConfigGroups(latest_config), Shards: CopyConfigShards(latest_config)}

	for k, v := range mapping {
		if _, exist := latest_config.Groups[k]; exist {
			panic("Trying to join already exist group")
		}
		new_config.Groups[k] = v
	}
	RebalanceShards(&new_config)
	cs.Configs = append(cs.Configs, new_config)
}

func (cs *ConfigState) Leave(gids []int) {
	latest_config := cs.LatestConfig()
	new_config := Config{Num: latest_config.Num + 1, Groups: CopyConfigGroups(latest_config), Shards: CopyConfigShards(latest_config)}

	for _, gid := range gids {
		if _, ok := new_config.Groups[gid]; !ok {
			panic(fmt.Sprintf("Leaving group:%d that was not joined", gid))
		}
		delete(new_config.Groups, gid)
	}

	for i, gid := range new_config.Shards {
		if _, ok := new_config.Groups[gid]; !ok {
			new_config.Shards[i] = 0
		}

	}
	RebalanceShards(&new_config)
	cs.Configs = append(cs.Configs, new_config)
}

func (cs *ConfigState) Move(shard, gid int) {
	latest_config := cs.LatestConfig()
	new_config := Config{Num: latest_config.Num + 1, Groups: CopyConfigGroups(latest_config), Shards: CopyConfigShards(latest_config)}

	if _, ok := new_config.Groups[gid]; !ok {
		panic("Try to move shard to non existent group")
	}
	new_config.Shards[shard] = gid

	cs.Configs = append(cs.Configs, new_config)
}

func (cs *ConfigState) Query(cid int) *Config {
	var result *Config
	if cid < 0 || cid >= len(cs.Configs) {
		result = cs.LatestConfig()
	} else {
		result = &cs.Configs[cid]
	}
	return result
}

func RebalanceShards(new_config *Config) {
	if len(new_config.Groups) == 0 {
		return
	}
	max_shard_per_group := math.Floor(float64(len(new_config.Shards)) / float64(len(new_config.Groups)))
	if max_shard_per_group == 0 {
		max_shard_per_group = 1
	}
	group_shard_count := getGroupShardCount(new_config)
	unfilled_groups := getUnfilledGroups(new_config, int(max_shard_per_group), group_shard_count)

	cur := 0
	for i, gid := range new_config.Shards {
		if gid == 0 || group_shard_count[gid] > int(max_shard_per_group) {
			if cur < len(unfilled_groups) {
				group_shard_count[gid]--
				unfilled_groups[cur].count++
				new_config.Shards[i] = unfilled_groups[cur].gid

				if unfilled_groups[cur].count >= int(max_shard_per_group) {
					cur++
				}
			} else if gid == 0 { //handoff extra shards due to round down
				wrap_cur := cur % len(unfilled_groups)
				unfilled_groups[wrap_cur].count++
				new_config.Shards[i] = unfilled_groups[wrap_cur].gid
				cur++
			}
		}
	}

}

func getGroupShardCount(config *Config) map[int]int {
	groups := map[int]int{}
	for _, gid := range config.Shards {
		if gid != 0 {
			groups[gid]++
		}
	}
	return groups
}

func getUnfilledGroups(config *Config, cap int, group_shard_count map[int]int) []GroupShardCount {
	groups := make([]GroupShardCount, 0, len(config.Groups))
	for k := range config.Groups {
		if group_shard_count[k] < cap {
			groups = append(groups, GroupShardCount{gid: k, count: group_shard_count[k]})
		}
	}
	return groups
}

func (cs *ConfigState) LatestConfig() *Config {
	return &cs.Configs[len(cs.Configs)-1]
}

func (cs *ConfigState) SetLatest(index, term int) {
	cs.LastIndex = index
	cs.LastTerm = term
}

func CopyConfigGroups(old_config *Config) map[int][]string {
	new_groups := map[int][]string{}
	for k, v := range old_config.Groups {
		new_groups[k] = v
	}
	return new_groups
}

func CopyConfigShards(old_config *Config) [NShards]int {
	return old_config.Shards
}
