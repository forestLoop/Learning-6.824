package kvraft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/keithnull/Learning-6.824/src/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	leaderID   int64 // remember the current leader
	logger     *log.Logger
	clerkID    string     // unique identifier for this clerk
	commandSeq int        // monotonically increasing command sequence number
	mu         sync.Mutex // for commandSeq only
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	id := uuid.New().String()
	ck := &Clerk{
		servers:    servers,
		leaderID:   0,
		logger:     log.New(os.Stdout, fmt.Sprintf("[Client %v]", id), log.Ltime|log.Lmicroseconds),
		clerkID:    id,
		commandSeq: 1,
		mu:         sync.Mutex{},
	}
	muteLoggerIfUnset(ck.logger, "debug_clerk")
	return ck
}

func (ck *Clerk) getLeader() int64 {
	return atomic.LoadInt64(&ck.leaderID)
}

func (ck *Clerk) changeLeader() {
	bias := rand.Int63n(int64(len(ck.servers)))
	oldLeader := ck.getLeader()
	newLeader := (oldLeader + bias) % int64(len(ck.servers))
	atomic.StoreInt64(&ck.leaderID, newLeader)
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
	for {
		targetServer := ck.getLeader()
		ck.logger.Printf("Try to contact server %v for Get: args = %v", targetServer, args)
		reply := GetReply{}
		ok := ck.servers[targetServer].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK { // success
			return reply.Value
		}
		// ready to retry
		if !ok {
			ck.logger.Printf("Server %v didn't respond for Get", targetServer)
		}
		if reply.Err != OK {
			ck.logger.Printf("Server %v responded with Error %v for Get", targetServer, reply.Err)
		}
		ck.changeLeader()
		time.Sleep(50 * time.Millisecond)
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
	for {
		targetServer := ck.getLeader()
		ck.logger.Printf("Try to contact server %v for PutAppend: args = %v", targetServer, args)
		reply := PutAppendReply{}
		ok := ck.servers[targetServer].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK { // success
			return
		}
		// ready to retry
		if !ok {
			ck.logger.Printf("Server %v didn't respond for PutAppend", targetServer)
		}
		if reply.Err != OK {
			ck.logger.Printf("Server %v responded with Error %v for PutAppend", targetServer, reply.Err)
		}
		ck.changeLeader()
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
