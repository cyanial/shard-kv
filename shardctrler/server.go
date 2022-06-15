package shardctrler

import (
	"sync"
	"time"

	"github.com/cyanial/raft"
	"github.com/cyanial/raft/labgob"
	"github.com/cyanial/raft/labrpc"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num

	waitApplyCh  map[int]chan Op
	lastApplySeq map[int64]int64
}

type Op struct {
	Method string

	// Join
	Servers map[int][]string // new GID -> servers mappings

	// Leave
	Gids []int

	// Move
	Shard int
	GID   int

	// Query
	Num int

	ClientId    int64
	SequenceNum int64
}

//
// The Join RPC is used by an administrator to add new replica groups. Its argument
// is a set of mappings from unique, non-zero replica group identifiers (GIDs) to
// list of server names. THe shardctrler should react by creating a new configuration
// that includes the new replica groups. The new configuration should divide the
// shards as evenly as possible among the full set of allow re-use of a GID if it's
// not part of the current configuration (i.e. a GID should be allowed to Join, then
// Leave, then Join again).
//
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {

	op := Op{
		Method:      "Join",
		Servers:     args.Servers,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	indexCh, exist := sc.waitApplyCh[index]
	if !exist {
		sc.waitApplyCh[index] = make(chan Op)
		indexCh = sc.waitApplyCh[index]
	}
	sc.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(Server_Timeout):
		if sc.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, index)
	sc.mu.Unlock()
	close(indexCh)
}

//
// The Leave RPC's argument is a list of GIDs of previously joined groups. The
// Shardctrler should create a new configuration that does not include those groups,
// and that assigns those group's shards to the remaining groups. The new configuration
// should divide the shards as evenly as possible among the groups, and should move
// as few shards as possible to achieve that goal.
//
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {

	op := Op{
		Method:      "Leave",
		Gids:        args.GIDs,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	indexCh, exist := sc.waitApplyCh[index]
	if !exist {
		sc.waitApplyCh[index] = make(chan Op)
		indexCh = sc.waitApplyCh[index]
	}
	sc.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(Server_Timeout):
		if sc.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, index)
	sc.mu.Unlock()
	close(indexCh)
}

//
// The Move RPC's arguments are a shard number and a GID. The shardctrler should
// create a new configuration in which the shard is assigned to the group. The
// purpose of Move is to allow us to test your software. A Join or Leave following
// a Move will likely un-do the Move, since Join and Leave re-balance.
//
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {

	op := Op{
		Method:      "Move",
		Shard:       args.Shard,
		GID:         args.GID,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	indexCh, exist := sc.waitApplyCh[index]
	if !exist {
		sc.waitApplyCh[index] = make(chan Op)
		indexCh = sc.waitApplyCh[index]
	}
	sc.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(Server_Timeout):
		if sc.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, index)
	sc.mu.Unlock()
	close(indexCh)
}

//
// The Query RPC's argument is a configuration number. The shardctrler replis with
// the configuration that has that number. If the number is -1 or bigger than the
// biggest known configuration number, the shardctrler should reply with the latest
// configuration. The result of Query(-1) should reflect every Join, Leave, or Move
// RPC that the shardctrler finished handling before it received the Query(-1) RPC.
//
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {

	op := Op{
		Method:      "Query",
		Num:         args.Num,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
	}

	index, _, isLeader := sc.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	indexCh, exist := sc.waitApplyCh[index]
	if !exist {
		sc.waitApplyCh[index] = make(chan Op)
		indexCh = sc.waitApplyCh[index]
	}
	sc.mu.Unlock()

	select {
	case commitOp := <-indexCh:
		if commitOp.ClientId == op.ClientId && commitOp.SequenceNum == op.SequenceNum {
			// reply.Config = getConfig?
			sc.mu.Lock()
			sc.lastApplySeq[op.ClientId] = op.SequenceNum
			if op.Num == -1 || op.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.Num]
			}
			sc.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	case <-time.After(Server_Timeout):
		_, isLeader := sc.rf.GetState()
		if sc.isDuplicatedCmd(op.ClientId, op.SequenceNum) && isLeader {
			// reply.Config = getConfig?
			sc.mu.Lock()
			sc.lastApplySeq[op.ClientId] = op.SequenceNum
			if op.Num == -1 || op.Num >= len(sc.configs) {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.Num]
			}
			sc.mu.Unlock()

			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}

	sc.mu.Lock()
	delete(sc.waitApplyCh, index)
	sc.mu.Unlock()
	close(indexCh)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			switch {

			case applyMsg.CommandValid:
				op := applyMsg.Command.(Op)

				if !sc.isDuplicatedCmd(op.ClientId, op.SequenceNum) {
					switch op.Method {

					case "Join":

						sc.mu.Lock()
						sc.lastApplySeq[op.ClientId] = op.SequenceNum
						sc.configs = append(sc.configs, sc.createJoinConfig(op.Servers))
						sc.mu.Unlock()

					case "Leave":

						sc.mu.Lock()
						sc.lastApplySeq[op.ClientId] = op.SequenceNum
						sc.configs = append(sc.configs, sc.createLeaveConfig(op.Gids))
						sc.mu.Unlock()

					case "Move":

						sc.mu.Lock()
						sc.lastApplySeq[op.ClientId] = op.SequenceNum
						// To-do:
						sc.configs = append(sc.configs, sc.createMoveConfig(op.Shard, op.GID))
						sc.mu.Unlock()

					case "Query":

					}

				}

				sc.mu.Lock()
				indexCh, exist := sc.waitApplyCh[applyMsg.CommandIndex]
				if exist {
					indexCh <- op
				}
				sc.mu.Unlock()

			case applyMsg.SnapshotValid:
				panic("no snapshot")
			}

		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {

	labgob.Register(Op{})

	sc := &ShardCtrler{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		configs:      make([]Config, 1),
		waitApplyCh:  make(map[int]chan Op),
		lastApplySeq: make(map[int64]int64),
	}

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.configs[0].Num = 0
	sc.configs[0].Shards = [NShards]int{}
	sc.configs[0].Groups = make(map[int][]string)

	go sc.applier()

	return sc
}
