package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group servers each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ShardComponent struct {
	ShardIndex   int
	ShardStore   map[string]string
	LastApplySeq map[int64]int64
}

type MoveShardArgs struct {
	ConfigNum int
	Data      []ShardComponent
}

type MoveShardReply struct {
	Err       Err
	ConfigNum int
}

// Put or Append
type PutAppendArgs struct {
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	ClientId    int64
	SequenceNum int64
	ConfigNum   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key         string
	ClientId    int64
	SequenceNum int64
	ConfigNum   int
}

type GetReply struct {
	Err   Err
	Value string
}
