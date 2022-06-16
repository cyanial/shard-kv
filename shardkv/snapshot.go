package shardkv

import (
	"bytes"

	"github.com/cyanial/raft"
	"github.com/cyanial/raft/labgob"
	"github.com/cyanial/shard-kv/shardctrler"
)

type PersistSnapshot struct {
	ShardStore   [shardctrler.NShards]map[string]string
	LastApplySeq [shardctrler.NShards]map[int64]int64
	ShardMoving  [shardctrler.NShards]bool
	Config       shardctrler.Config
}

func (kv *ShardKV) GetSnapshotFromRaft(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
		kv.InstallSnapshot(applyMsg.Snapshot)
		kv.snapshotIndex = applyMsg.SnapshotIndex
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) CreateSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	snapshot := PersistSnapshot{
		ShardStore:   kv.shardStore,
		LastApplySeq: kv.lastApplySeq,
		ShardMoving:  kv.shardMoving,
		Config:       kv.config,
	}

	err := e.Encode(snapshot)
	if err != nil {
		return nil
	}

	return w.Bytes()
}

func (kv *ShardKV) InstallSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistSnapshot PersistSnapshot

	err := d.Decode(&persistSnapshot)
	if err != nil {
		return
	}

	kv.shardStore = persistSnapshot.ShardStore
	kv.lastApplySeq = persistSnapshot.LastApplySeq
	kv.shardMoving = persistSnapshot.ShardMoving
	kv.config = persistSnapshot.Config
}
