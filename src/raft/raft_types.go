package raft

import (
	"log"
	"sync"
	"time"

	"github.com/keithnull/Learning-6.824/src/labrpc"
)

const (
	electionTimeoutMin int = 700  // ms
	electionTimeoutMax int = 1000 // ms
	checkPeriod            = 50 * time.Millisecond
	heartbeatInterval      = 120 * time.Millisecond
	maxAttempts            = 1 // max attempts for each RPC request
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool // true if the ApplyMsg contains a newly committed log entry
	Command      interface{}
	CommandIndex int // the index of this command in log
	CommandTerm  int // the term of this command when first added
}

// State is the role of a Raft peer, must be one of follower, candidate and leader
type State int

// All three valid states
const (
	Follower State = iota
	Candidate
	Leader
)

// LogEntry is an entry in log
type LogEntry struct {
	Term    int         // term when entry was received by leader (first index is 1)
	Command interface{} // command for state machine
}

// Raft is the Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	state        State               // this peer's state
	nextDeadline time.Time           // the next deadline before which a headbeat should be received
	votes        int                 // votes received as a candidate
	logger       *log.Logger         // logger for this Raft server
	applyCh      chan ApplyMsg       // channel for apply messages
	applyCond    *sync.Cond          // condition variable to notify the change of commitIndex
	leaderCond   *sync.Cond          // condition variable to notify the state conversion to leader

	// Persistent state on all servers
	currentTerm int         // latest term this peer has seen (initialized to 0 on first boot and increases monotonically)
	votedFor    int         // candidate that received vote in current term or -1 if not voted
	log         []*LogEntry // log entries on this server

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited (initialized to 0 and increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0 and increases monotonically)

	// Volatile state on leaders only
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to the leader's last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on that server (initialized to 0 and increases monotonically)
}
