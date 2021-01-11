package kvraft

import (
	"fmt"
	"log"
	mathrand "math/rand"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/keithnull/Learning-6.824/src/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int // -1 for unknown
	logger     *log.Logger
	clerkID    string     // unique identifier for this clerk
	commandSeq int        // monotonically increasing command sequence number
	mu         sync.Mutex // for commandSeq only
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	id := uuid.New().String()
	ck := &Clerk{
		servers:    servers,
		lastLeader: -1,
		logger:     log.New(os.Stdout, fmt.Sprintf("[Client %v]", id), log.Ltime|log.Lmicroseconds),
		clerkID:    id,
		commandSeq: 1,
		mu:         sync.Mutex{},
	}
	muteLoggerIfUnset(ck.logger, "debug_clerk")
	return ck
}

func (ck *Clerk) getLastLeader() int {
	if ck.lastLeader != -1 {
		return ck.lastLeader
	}
	return mathrand.Int() % len(ck.servers)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	args := GetArgs{
		Key:     key,
		ClerkID: ck.clerkID,
		Seq:     ck.commandSeq,
	}
	ck.commandSeq++
	ck.mu.Unlock()
	targetServer := ck.getLastLeader() - 1
	for {
		targetServer = (targetServer + 1) % len(ck.servers)
		ck.logger.Printf("Try to contact server %v for Get: args = %v", targetServer, args)
		reply := GetReply{}
		ok := ck.servers[targetServer].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.logger.Printf("Server %v didn't respond for Get", targetServer)
			continue
		}
		if reply.Err != OK {
			ck.logger.Printf("Server %v responded with Error %v for Get", targetServer, reply.Err)
			continue
		}
		ck.lastLeader = targetServer
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkID: ck.clerkID,
		Seq:     ck.commandSeq,
	}
	ck.commandSeq++
	ck.mu.Unlock()
	targetServer := ck.getLastLeader() - 1
	for {
		targetServer = (targetServer + 1) % len(ck.servers)
		ck.logger.Printf("Try to contact server %v for PutAppend: args = %v", targetServer, args)
		reply := PutAppendReply{}
		ok := ck.servers[targetServer].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.logger.Printf("Server %v didn't respond for PutAppend", targetServer)
			continue
		}
		if reply.Err != OK {
			ck.logger.Printf("Server %v responded with Error %v for PutAppend", targetServer, reply.Err)
			continue
		}
		ck.lastLeader = targetServer
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
