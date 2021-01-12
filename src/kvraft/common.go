package kvraft

import (
	"io/ioutil"
	"log"
	"os"
)

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrFailedToApply = "ErrFailedToApply"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	ClerkID string // clerk's unique id
	Seq     int    // for each clerk, its monotonically increasing command sequence number
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key     string
	ClerkID string // clerk's unique id
	Seq     int    // for each clerk, its monotonically increasing command sequence number
}

type GetReply struct {
	Err   Err
	Value string
}

func muteLoggerIfUnset(logger *log.Logger, env string) {
	if os.Getenv(env) == "" {
		muteLogger(logger)
	}
}

func muteLogger(logger *log.Logger) {
	logger.SetOutput(ioutil.Discard)
}
