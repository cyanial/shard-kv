package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/cyanial/raft/labrpc"
	"github.com/cyanial/shard-kv/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId    int64
	sequenceNum int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:          shardctrler.MakeClerk(ctrlers),
		make_end:    make_end,
		clientId:    nrand(),
		sequenceNum: 0,
	}

	ck.config = ck.sm.Query(-1)

	DPrintf("Init Clerk: %d, Config #%d", ck.clientId, ck.config.Num)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{
		Key:         key,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
		ConfigNum:   ck.config.Num,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])

				DPrintf("[Client %d, To %s] Get, shard=%d, gid=%d, k=%s - ",
					ck.clientId, servers[si], shard, gid, args.Key)

				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("[Client %d, To %s] Get, shard=%d, gid=%d, k=%s, v=%s - OK||No Key",
						ck.clientId, servers[si], shard, gid, args.Key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					DPrintf("[Client %d, To %s] Get, shard=%d, gid=%d, k=%s - Wrong Group",
						ck.clientId, servers[si], shard, gid, args.Key)
					break
				}
				DPrintf("[Client %d, To %s] Get, shard=%d, gid=%d, k=%s - Wrong Leader",
					ck.clientId, servers[si], shard, gid, args.Key)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		DPrintf("[Client %d] Query Config #%d",
			ck.clientId, ck.config.Num)

		args.ConfigNum = ck.config.Num
	}

}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:         key,
		Value:       value,
		Op:          op,
		ClientId:    ck.clientId,
		SequenceNum: atomic.AddInt64(&ck.sequenceNum, 1),
		ConfigNum:   ck.config.Num,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])

				DPrintf("[Client %d, To %s] %s, shard=%d, gid=%d, k=%s, v=%s - ",
					ck.clientId, servers[si], args.Op, shard, gid, args.Key, args.Value)

				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("[Client %d, To %s] %s, shard=%d, gid=%d, k=%s, v=%s - OK",
						ck.clientId, servers[si], args.Op, shard, gid, args.Key, args.Value)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("[Client %d, To %s] %s, shard=%d, gid=%d, k=%s, v=%s - Wrong Group",
						ck.clientId, servers[si], args.Op, shard, gid, args.Key, args.Value)
					break
				}
				DPrintf("[Client %d, To %s] %s, shard=%d, gid=%d, k=%s, v=%s - Wrong Leader",
					ck.clientId, servers[si], args.Op, shard, gid, args.Key, args.Value)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		DPrintf("[Client %d] Query Config #%d",
			ck.clientId, ck.config.Num)

		args.ConfigNum = ck.config.Num
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
