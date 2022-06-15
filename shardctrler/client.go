package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cyanial/raft/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	mu       sync.Mutex
	leaderId int

	clientId    int64
	sequenceNum int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	ck := &Clerk{
		servers:     servers,
		leaderId:    0,
		clientId:    nrand(),
		sequenceNum: 0,
	}

	return ck
}

func (ck *Clerk) Query(num int) Config {

	args := &QueryArgs{
		Num:         num,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}

	leaderId := ck.currentLeader()

	for {

		reply := &QueryReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Query", args, reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}

		leaderId = ck.changeLeader()

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	args := &JoinArgs{
		Servers:     servers,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}

	leaderId := ck.currentLeader()

	for {

		reply := &JoinReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Join", args, reply)
		if ok && reply.WrongLeader == false {
			return
		}

		leaderId = ck.changeLeader()

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {

	args := &LeaveArgs{
		GIDs:        gids,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}

	leaderId := ck.currentLeader()

	for {

		reply := &LeaveReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Leave", args, reply)
		if ok && reply.WrongLeader == false {
			return
		}

		leaderId = ck.changeLeader()

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	args := &MoveArgs{
		Shard:       shard,
		GID:         gid,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
	}

	leaderId := ck.currentLeader()

	for {

		reply := &MoveReply{}
		ok := ck.servers[leaderId].Call("ShardCtrler.Move", args, reply)
		if ok && reply.WrongLeader == false {
			return
		}

		leaderId = ck.changeLeader()

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderId
}

func (ck *Clerk) changeLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	return ck.leaderId
}
