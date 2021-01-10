package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	mathrand "math/rand"
	"os"

	"github.com/keithnull/Learning-6.824/src/labrpc"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int // -1 for unknown
	logger     *log.Logger
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:    servers,
		lastLeader: -1,
		logger:     log.New(os.Stdout, "[Client]", log.Ltime|log.Lmicroseconds),
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
	args := GetArgs{
		Key: key,
	}
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
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
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
