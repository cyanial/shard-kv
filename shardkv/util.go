package shardkv

import (
	"log"
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
