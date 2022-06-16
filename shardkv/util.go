package shardkv

import (
	"log"
	"time"

	"github.com/cyanial/raft"
	"github.com/cyanial/shard-kv/shardctrler"
)

const Debug = false

const (
	PollConfig_Timeout = 100 * time.Millisecond
	SendShard_Timeout  = 150 * time.Millisecond
	Server_Timeout     = 1000 * time.Millisecond
)

func init() {
	log.SetFlags(log.Ltime)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) CheckShardState(configNum, shardIndex int) (bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	serve := kv.config.Num == configNum && kv.config.Shards[shardIndex] == kv.gid
	avail := !kv.shardMoving[shardIndex]

	return serve, avail
}

func (kv *ShardKV) CheckMoveState(shardComponents []ShardComponent) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, shardData := range shardComponents {
		if kv.shardMoving[shardData.ShardIndex] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) isDuplicatedCmd(clientId, sequenceNum int64, shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastApplyNum, hasClient := kv.lastApplySeq[shard][clientId]
	if !hasClient {
		// has no client
		return false
	}
	return sequenceNum <= lastApplyNum
}

func (kv *ShardKV) applyConfig(config shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if config.Num != kv.config.Num+1 {
		return
	}

	// all migration should be finished
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.shardMoving[shard] {
			return
		}
	}

	kv.lockShardMoving(config.Shards)
	kv.config = config
}

func (kv *ShardKV) lockShardMoving(newShards [shardctrler.NShards]int) {

	oldShards := kv.config.Shards
	for shard := 0; shard < shardctrler.NShards; shard++ {
		// new Shards own to myself
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			if newShards[shard] != 0 {
				kv.shardMoving[shard] = true
			}
		}
		// old Shards not ever belong to myself
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			if oldShards[shard] != 0 {
				kv.shardMoving[shard] = true
			}
		}
	}
}

func (kv *ShardKV) ProcessCommand(applyMsg raft.ApplyMsg) {

	if applyMsg.CommandIndex <= kv.snapshotIndex {
		return
	}

	op := applyMsg.Command.(Op)

	if op.Method == "Config" {

		DPrintf("[Server %d] Config #%d",
			kv.me, op.Config.Num)

		kv.applyConfig(op.Config)
		if kv.maxraftstate != -1 {
			if kv.rf.RaftStateSize() > kv.maxraftstate {
				snapshot := kv.CreateSnapshot()
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}
		}
		return
	}

	if op.Method == "MoveShard" {
		DPrintf("[Server %d] Move Shard",
			kv.me)

		kv.applyShard(op)

		if kv.maxraftstate != -1 {
			if kv.rf.RaftStateSize() > kv.maxraftstate {
				snapshot := kv.CreateSnapshot()
				kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
			}
		}
		// reply
		kv.mu.Lock()
		indexCh, exist := kv.waitApplyCh[applyMsg.CommandIndex]
		if exist {
			indexCh <- op
		}
		kv.mu.Unlock()
		return
	}

	if !kv.isDuplicatedCmd(op.ClientId, op.SequenceNum, key2shard(op.Key)) {
		switch op.Method {

		case "Get":

		case "Put":
			DPrintf("[Server %d] Apply Put, k=%s, v=%s",
				kv.me, op.Key, op.Value)

			kv.mu.Lock()
			shard := key2shard(op.Key)
			kv.shardStore[shard][op.Key] = op.Value
			kv.lastApplySeq[shard][op.ClientId] = op.SequenceNum
			kv.mu.Unlock()

		case "Append":
			DPrintf("[Server %d] Apply Append, k=%s, v=%s",
				kv.me, op.Key, op.Value)

			kv.mu.Lock()
			shard := key2shard(op.Key)
			kv.shardStore[shard][op.Key] += op.Value
			kv.lastApplySeq[shard][op.ClientId] = op.SequenceNum
			kv.mu.Unlock()

		}
	}

	if kv.maxraftstate != -1 {
		if kv.rf.RaftStateSize() > kv.maxraftstate {
			snapshot := kv.CreateSnapshot()
			kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
		}
	}

	// reply
	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[applyMsg.CommandIndex]
	if exist {
		indexCh <- op
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) haveSendData() (bool, map[int][]ShardComponent) {

	kv.mu.Lock()
	sendData := make(map[int][]ShardComponent)
	for shard := 0; shard < shardctrler.NShards; shard++ {
		nowOwner := kv.config.Shards[shard]
		if kv.shardMoving[shard] && kv.gid != nowOwner {
			tempComponent := ShardComponent{
				ShardIndex:   shard,
				ShardStore:   make(map[string]string),
				LastApplySeq: make(map[int64]int64),
			}

			// clone
			for key, value := range kv.shardStore[shard] {
				tempComponent.ShardStore[key] = value
			}
			for clientId, sequenceNum := range kv.lastApplySeq[shard] {
				tempComponent.LastApplySeq[clientId] = sequenceNum
			}

			sendData[nowOwner] = append(sendData[nowOwner], tempComponent)
		}
	}
	kv.mu.Unlock()

	if len(sendData) == 0 {
		return false, sendData
	}

	return true, sendData
}

func (kv *ShardKV) sendShard(sendData map[int][]ShardComponent) {
	for aimGid, shardComponents := range sendData {
		kv.mu.Lock()
		args := &MoveShardArgs{
			ConfigNum: kv.config.Num,
			Data:      make([]ShardComponent, 0),
		}
		groupServers := kv.config.Groups[aimGid]
		kv.mu.Unlock()
		for _, component := range shardComponents {
			tempComponent := ShardComponent{
				ShardIndex:   component.ShardIndex,
				ShardStore:   make(map[string]string),
				LastApplySeq: make(map[int64]int64),
			}

			// clone
			for key, value := range component.ShardStore {
				tempComponent.ShardStore[key] = value
			}
			for clientId, sequenceNum := range component.LastApplySeq {
				tempComponent.LastApplySeq[clientId] = sequenceNum
			}
			args.Data = append(args.Data, tempComponent)
		}

		go kv.sendShardToOtherGroups(groupServers, args)
	}
}

func (kv *ShardKV) sendShardToOtherGroups(groupServers []string, args *MoveShardArgs) {
	for _, groupMember := range groupServers {
		srv := kv.make_end(groupMember)
		reply := &MoveShardReply{}
		ok := srv.Call("ShardKV.MoveShard", args, reply)
		if !ok {
			return
		}

		kv.mu.Lock()
		myConfigNum := kv.config.Num
		kv.mu.Unlock()
		if ok && reply.Err == OK {
			// Success
			if myConfigNum != args.ConfigNum || kv.CheckMoveState(args.Data) {
				return
			} else {
				kv.rf.Start(Op{
					Method:    "MoveShard",
					ShardData: args.Data,
					ConfigNum: args.ConfigNum,
				})
				return
			}
		}
	}
}

func (kv *ShardKV) applyShard(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	myConfig := kv.config
	if op.ConfigNum != myConfig.Num {
		return
	}

	for _, shardComponent := range op.ShardData {
		if !kv.shardMoving[shardComponent.ShardIndex] {
			continue
		}
		kv.shardMoving[shardComponent.ShardIndex] = false

		kv.shardStore[shardComponent.ShardIndex] = make(map[string]string)
		kv.lastApplySeq[shardComponent.ShardIndex] = make(map[int64]int64)

		if myConfig.Shards[shardComponent.ShardIndex] == kv.gid {
			// clone
			for key, value := range shardComponent.ShardStore {
				kv.shardStore[shardComponent.ShardIndex][key] = value
			}

			for clientId, sequenceNum := range shardComponent.LastApplySeq {
				kv.lastApplySeq[shardComponent.ShardIndex][clientId] = sequenceNum
			}
		}

	}
}
