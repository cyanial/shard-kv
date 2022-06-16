package shardkv

import (
	"sync"
	"time"

	"github.com/cyanial/raft"
	"github.com/cyanial/raft/labgob"
	"github.com/cyanial/raft/labrpc"
	"github.com/cyanial/shard-kv/shardctrler"
)

type Op struct {
	Method string

	// K/V service
	Key   string
	Value string

	// Poll Config
	Config shardctrler.Config

	// Move Shard
	ShardData []ShardComponent
	ConfigNum int

	ClientId    int64
	SequenceNum int64
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	gid      int
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	ctrlers  []*labrpc.ClientEnd
	mck      *shardctrler.Clerk
	config   shardctrler.Config

	maxraftstate int // snapshot if log grows this big

	shardMoving  [shardctrler.NShards]bool
	shardStore   [shardctrler.NShards]map[string]string
	lastApplySeq [shardctrler.NShards]map[int64]int64

	waitApplyCh map[int]chan Op

	snapshotIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)

	serve, avail := kv.CheckShardState(args.ConfigNum, shard)

	if !serve {
		reply.Err = ErrWrongGroup
		return
	}

	if !avail {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Method:      "Get",
		Key:         args.Key,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op)
		indexCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {

			kv.mu.Lock()
			shard := key2shard(op.Key)
			value, has := kv.shardStore[shard][op.Key]
			kv.lastApplySeq[shard][op.ClientId] = op.SequenceNum
			kv.mu.Unlock()

			if has {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}

	case <-time.After(Server_Timeout):

		_, isLeader := kv.rf.GetState()
		if kv.isDuplicatedCmd(op.ClientId, op.SequenceNum, key2shard(op.Key)) && isLeader {
			kv.mu.Lock()
			shard := key2shard(op.Key)
			value, has := kv.shardStore[shard][op.Key]
			kv.lastApplySeq[shard][op.ClientId] = op.SequenceNum
			kv.mu.Unlock()

			if has {
				reply.Err = OK
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	close(indexCh)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("[Server %d] %s, Wrong Leader - first",
			kv.me, args.Op)
		reply.Err = ErrWrongLeader
		return
	}

	shard := key2shard(args.Key)

	serve, avail := kv.CheckShardState(args.ConfigNum, shard)

	if !serve {
		DPrintf("[Server %d] %s, Wrong Group",
			kv.me, args.Op)
		reply.Err = ErrWrongGroup
		return
	}

	if !avail {
		DPrintf("[Server %d] %s, Wrong Leader - avail",
			kv.me, args.Op)
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Method:      args.Op,
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op)
		indexCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			reply.Err = OK
		} else {
			DPrintf("[Server %d] %s, Wrong Leader - commitop",
				kv.me, args.Op)

			reply.Err = ErrWrongLeader
		}
	case <-time.After(Server_Timeout):
		if kv.isDuplicatedCmd(op.ClientId, op.SequenceNum, shard) {
			reply.Err = OK
		} else {
			DPrintf("[Server %d] %s, Wrong Leader - timeout",
				kv.me, args.Op)
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	close(indexCh)
}

// RPC handler
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) {

	kv.mu.Lock()
	myConfigNum := kv.config.Num
	kv.mu.Unlock()
	if args.ConfigNum > myConfigNum {
		reply.ConfigNum = myConfigNum
		return
	}

	if args.ConfigNum < myConfigNum {
		reply.Err = OK
		return
	}

	if kv.CheckMoveState(args.Data) {
		reply.Err = OK
		return
	}

	op := Op{
		Method:    "MoveShard",
		ShardData: args.Data,
		ConfigNum: args.ConfigNum,
	}

	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	indexCh, exist := kv.waitApplyCh[index]
	if !exist {
		kv.waitApplyCh[index] = make(chan Op)
		indexCh = kv.waitApplyCh[index]
	}
	kv.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		kv.mu.Lock()
		tempConfigNum := kv.config.Num
		kv.mu.Unlock()
		if commitOp.ConfigNum == args.ConfigNum && args.ConfigNum <= tempConfigNum &&
			kv.CheckMoveState(args.Data) {
			reply.ConfigNum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}

	case <-time.After(Server_Timeout):

		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		tempConfigNum := kv.config.Num
		kv.mu.Unlock()
		if args.ConfigNum <= tempConfigNum && kv.CheckMoveState(args.Data) &&
			isLeader {
			reply.ConfigNum = tempConfigNum
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, index)
	kv.mu.Unlock()
	close(indexCh)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) configPoller() {
	for {
		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader {
			time.Sleep(PollConfig_Timeout)
			continue
		}

		newConfig := kv.mck.Query(lastConfigNum + 1)
		if newConfig.Num == lastConfigNum+1 {
			// a new config
			op := Op{
				Method: "Config",
				Config: newConfig,
			}

			kv.mu.Lock()
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.rf.Start(op)
			}
			kv.mu.Unlock()
		}

		time.Sleep(PollConfig_Timeout)
	}
}

func (kv *ShardKV) applier() {
	for applyMsg := range kv.applyCh {

		if applyMsg.CommandValid {
			kv.ProcessCommand(applyMsg)

		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				kv.InstallSnapshot(applyMsg.Snapshot)
				kv.snapshotIndex = applyMsg.SnapshotIndex
			}
			kv.mu.Unlock()
		}

	}
}

func (kv *ShardKV) shardSender() {
	for {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		kv.mu.Unlock()

		if !isLeader {
			time.Sleep(SendShard_Timeout)
			continue
		}

		noMove := true
		kv.mu.Lock()
		for shard := 0; shard < shardctrler.NShards; shard++ {
			if kv.shardMoving[shard] {
				noMove = false
			}
		}
		kv.mu.Unlock()

		if noMove {
			time.Sleep(SendShard_Timeout)
			continue
		}

		needSend, sendData := kv.haveSendData()
		if !needSend {
			time.Sleep(SendShard_Timeout)
			continue
		}

		kv.sendShard(sendData)

		time.Sleep(SendShard_Timeout)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &ShardKV{
		me:            me,
		gid:           gid,
		applyCh:       make(chan raft.ApplyMsg),
		make_end:      make_end,
		ctrlers:       ctrlers,
		maxraftstate:  maxraftstate,
		shardMoving:   [shardctrler.NShards]bool{},
		waitApplyCh:   make(map[int]chan Op),
		snapshotIndex: 0,
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardStore[i] = make(map[string]string)
		kv.lastApplySeq[i] = make(map[int64]int64)
	}

	if persister.SnapshotSize() > 0 {
		// install snapshot
		kv.InstallSnapshot(persister.ReadSnapshot())
	}

	// start configPoller
	go kv.configPoller()

	// start applier
	go kv.applier()

	// start shard sender
	go kv.shardSender()

	return kv
}
