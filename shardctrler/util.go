package shardctrler

import (
	"log"
	"sort"
	"time"
)

const Debug = false

const Server_Timeout = 1000 * time.Millisecond

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) isDuplicatedCmd(clientId, sequenceNum int64) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	lastApplyNum, hasClient := sc.lastApplySeq[clientId]
	if !hasClient {
		// has no client
		return false
	}
	return sequenceNum <= lastApplyNum
}

func isAvg(length, subNum, i int) bool {
	if i < length-subNum {
		return true
	}
	return false
}

func (sc *ShardCtrler) reBalanceShards(gid2shardCount map[int]int, lastShards [NShards]int) [NShards]int {

	length := len(gid2shardCount)
	avg, rem := NShards/length, NShards%length
	gids := make([]int, 0, length)
	for gid := range gid2shardCount {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gid2shardCount[gids[i]] < gid2shardCount[gids[j]] ||
			(gid2shardCount[gids[i]] == gid2shardCount[gids[j]] && gids[i] < gids[j])
	})

	for i := length - 1; i >= 0; i-- {
		resultNum := avg
		if !isAvg(length, rem, i) {
			resultNum = avg + 1
		}
		if resultNum < gid2shardCount[gids[i]] {
			fromGid := gids[i]
			changeNum := gid2shardCount[fromGid] - resultNum
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					lastShards[shard] = 0
					changeNum -= 1
				}
			}
			gid2shardCount[fromGid] = resultNum
		}
	}

	for i := 0; i < length; i++ {
		resultNum := avg
		if !isAvg(length, rem, i) {
			resultNum = avg + 1
		}
		if resultNum > gid2shardCount[gids[i]] {
			toGid := gids[i]
			changeNum := resultNum - gid2shardCount[toGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = toGid
					changeNum -= 1
				}
			}
			gid2shardCount[toGid] = resultNum
		}

	}
	return lastShards
}

func (sc *ShardCtrler) createJoinConfig(Servers map[int][]string) Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	for gid, servers := range lastConfig.Groups {
		tempGroups[gid] = servers
	}

	for gid, servers := range Servers {
		tempGroups[gid] = servers
	}

	gid2shardCount := make(map[int]int)
	for gid := range tempGroups {
		gid2shardCount[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			gid2shardCount[gid]++
		}
	}

	if len(gid2shardCount) == 0 {
		return Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: tempGroups,
		}
	}

	return Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(gid2shardCount, lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) createLeaveConfig(Gids []int) Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	leaveGids := make(map[int]bool)
	for _, gid := range Gids {
		leaveGids[gid] = true
	}

	for gid, servers := range lastConfig.Groups {
		tempGroups[gid] = servers
	}

	for _, gid := range Gids {
		delete(tempGroups, gid)
	}

	newShard := lastConfig.Shards
	gid2shardCount := make(map[int]int)
	for gid := range tempGroups {
		if !leaveGids[gid] {
			gid2shardCount[gid] = 0
		}
	}

	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if leaveGids[gid] {
				newShard[shard] = 0
			} else {
				gid2shardCount[gid]++
			}
		}
	}

	if len(gid2shardCount) == 0 {
		return Config{
			Num:    len(sc.configs),
			Shards: [NShards]int{},
			Groups: tempGroups,
		}
	}

	return Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(gid2shardCount, newShard),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) createMoveConfig(Shard, Gid int) Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{
		Num:    len(sc.configs),
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}
	tempConfig.Shards[Shard] = Gid

	for gid, servers := range lastConfig.Groups {
		tempConfig.Groups[gid] = servers
	}

	return tempConfig
}
