package kvraft

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/keithnull/Learning-6.824/src/labgob"
	"github.com/keithnull/Learning-6.824/src/labrpc"
	"github.com/keithnull/Learning-6.824/src/raft"
)

type Op struct {
	Type  string
	Key   string
	Value string
}

type waitingEntry struct {
	term    int
	cond    *sync.Cond
	success bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	logger  *log.Logger

	database map[string]string

	maxraftstate int // snapshot if log grows this big

	waitingRequest map[int]*waitingEntry
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.logger.Printf("RPC request: %v", args)
	defer kv.logger.Printf("RPC reply: %v", reply)
	index, term, isLeader := kv.rf.Start(Op{
		Type:  "Get",
		Key:   args.Key,
		Value: "",
	})
	reply.Err = OK
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logger.Printf("Sorry, I'm not a leader.")
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cond := sync.NewCond(&kv.mu)
	kv.waitingRequest[index] = &waitingEntry{
		term:    term,
		cond:    cond,
		success: false,
	}
	defer delete(kv.waitingRequest, index) // remember to free memory space!
	kv.logger.Printf("Wait for applied messages: key = %v", index)
	// wait till this command is applied
	cond.Wait()
	kv.logger.Printf("I'm now awake: key = %v", index)
	if !kv.waitingRequest[index].success {
		reply.Err = ErrFailedToApply
		kv.logger.Printf("Failed to apply")
		return
	}
	reply.Value = kv.database[args.Key]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.logger.Printf("RPC request: %v", args)
	defer kv.logger.Printf("RPC reply: %v", reply)
	index, term, isLeader := kv.rf.Start(Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
	})
	reply.Err = OK
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logger.Printf("Sorry, I'm not a leader.")
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cond := sync.NewCond(&kv.mu)
	kv.waitingRequest[index] = &waitingEntry{
		term:    term,
		cond:    cond,
		success: false,
	}
	defer delete(kv.waitingRequest, index) // remember to free memory space!
	kv.logger.Printf("Wait for applied messages: key = %v", index)
	// wait till this command is applied
	cond.Wait()
	kv.logger.Printf("I'm now awake: key = %v", index)
	if !kv.waitingRequest[index].success {
		reply.Err = ErrFailedToApply
		kv.logger.Printf("Failed to apply")
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyCommandDaemon() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.logger.Printf("Get a message to apply: %v", msg)
			if !msg.CommandValid {
				continue
			}
			op, ok := msg.Command.(Op)
			if !ok {
				kv.logger.Printf("Invalid operation: %v", msg.Command)
				continue
			}
			kv.logger.Printf("Valid operation: %v", op)
			switch op.Type {
			case "Get":
				// nothing to do for Get here
			case "Put":
				kv.mu.Lock()
				kv.database[op.Key] = op.Value
				kv.mu.Unlock()
			case "Append":
				kv.mu.Lock()
				kv.database[op.Key] += op.Value
				kv.mu.Unlock()
			default:
				// unknown operation
				kv.logger.Printf("Unknown operation type: %v", op.Type)
			}
			kv.mu.Lock()
			if we, ok := kv.waitingRequest[msg.CommandIndex]; ok {
				kv.logger.Printf("Wake up cond for %v: %v", msg.CommandIndex, we)
				// test if the terms match
				we.success = (msg.CommandTerm == we.term)
				we.cond.Broadcast()
			}
			kv.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		mu:             sync.Mutex{},
		me:             me,
		applyCh:        applyCh,
		rf:             raft.Make(servers, me, persister, applyCh),
		dead:           0,
		logger:         log.New(os.Stdout, fmt.Sprintf("[Server %v]", me), log.Ltime|log.Lmicroseconds),
		database:       make(map[string]string),
		waitingRequest: make(map[int]*waitingEntry),
		maxraftstate:   maxraftstate,
	}
	muteLoggerIfUnset(kv.logger, "debug_server")
	go kv.applyCommandDaemon()
	return kv
}
