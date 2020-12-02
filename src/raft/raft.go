package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/keithnull/Learning-6.824/src/labrpc"
)

// import "bytes"
// import "github.com/keithnull/Learning-6.824/src/labgob"

const (
	electionTimeoutMin int = 700  // ms
	electionTimeoutMax int = 1000 // ms
	checkPeriod            = 50 * time.Millisecond
	heartbeatInterval      = 120 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	leaderCond   *sync.Cond
	// Persistent state on all servers
	currentTerm int         // latest term this peer has seen (initialized to 0 on first boot and increases monotonically)
	votedFor    *int        // candidate that received vote in current term or nil if none
	log         []*LogEntry // log entries on this server

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited (initialized to 0 and increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0 and increases monotonically)

	// Volatile state on leaders only
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to the leader's last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on that server (initialized to 0 and increases monotonically)
}

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVoteArgs is the RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply is the RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //  receiver's current term
	VoteGranted bool // whether  receiver granted its vote to the candidate
}

// RequestVote is the RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start to handle incoming RequestVote RPC: args = %v", args)
	defer rf.logger.Printf("Finish handling incoming RequestVote RPC: reply = %v", reply)
	// Rule 2 for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.logger.Printf("Convert to follower: currentTerm = %v -> %v", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = nil // reset votedFor as it's a new term
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false       // by default, deny the request
	if args.Term < rf.currentTerm { // candidate's term is older than receiver
		rf.logger.Printf("args.Term (%v) < currentTerm (%v)", args.Term, rf.currentTerm)
		return
	}
	if rf.votedFor != nil && *rf.votedFor != args.CandidateID { //  receiver has already voted for another candidate
		rf.logger.Printf("Already voted for another candidate: votedFor = %v, candidateID = %v", *rf.votedFor, args.CandidateID)
		return
	}
	// 1. receiver has no log or its last log term is older than candidate's last log term
	isLogUpToDate := len(rf.log) == 0 || rf.log[len(rf.log)-1].Term < args.LastLogTerm
	// 2. receiver's last log term is the same as candidate's last log term, but its log is shorter
	isLogUpToDate = isLogUpToDate || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) <= args.LastLogIndex)
	rf.logger.Printf("Candidate's log is at least as up-to-date as my log? %v", isLogUpToDate)
	reply.VoteGranted = isLogUpToDate // grant vote if candidate's log is at least as up-to-date as receiver's log
	if reply.VoteGranted {
		rf.votedFor = &args.CandidateID
		rf.resetElectionTimer()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs is the AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderID     int         // so followers can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of PrevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int         // leader's commitIndex
}

// AppendEntriesReply is the AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // receiver's term, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// AppendEntries is the AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start to handle incoming AppendEntries RPC: args = %v", args)
	defer rf.logger.Printf("Finish handling incoming AppendEntries RPC: reply = %v", reply)
	// Rule 2 for all servers in Figure 2
	if args.Term > rf.currentTerm {
		rf.logger.Printf("Convert to follower: currentTerm = %v -> %v", rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = nil // reset votedFor as it's a new term
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.logger.Printf("args.Term (%v) < currentTerm (%v)", args.Term, rf.currentTerm)
		return
	}
	// till now, we are sure this RPC is from the current leader
	rf.resetElectionTimer()
	// reply false if log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm
	matched := (args.PrevLogIndex == 0 && len(rf.log) == 0)
	matched = matched || (1 <= args.PrevLogIndex && args.PrevLogIndex <= len(rf.log) && rf.log[args.PrevLogIndex-1].Term == args.PrevLogTerm)
	if !matched {
		var entry *LogEntry = nil
		if len(rf.log) >= args.PrevLogIndex {
			entry = rf.log[args.PrevLogIndex-1]
		}
		rf.logger.Printf("Log doesn't contain a matching entry at PrevLogIndex: PrevLogTerm = %v, log entry = %v", args.PrevLogTerm, entry)
		return
	}
	// add these entries
	oldLog := rf.log
	for i, entry := range args.Entries {
		if len(rf.log) <= i+args.PrevLogIndex { // out of existing log entries, simply append it
			rf.log = append(rf.log, entry)
		} else { // still within existing log entries
			if entry.Term != rf.log[args.PrevLogIndex+i].Term { // conflicting entry
				rf.log = rf.log[:args.PrevLogIndex+i] // delete the existing entry and all that follow it
				rf.log = append(rf.log, entry)
			}
			// otherwise, do nothing to this entry
		}
	}
	rf.logger.Printf("Log entries: %v -> %v", oldLog, rf.log)
	if args.LeaderCommit > rf.commitIndex {
		// take the min of LeaderCommit and index of last new entry
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		rf.logger.Printf("Update commitIndex: %v -> %v", oldCommitIndex, rf.commitIndex)
	}
	reply.Success = true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == Leader)
	if !isLeader {
		index, term = -1, -1
		rf.logger.Printf("Sorry but I'm not the leader, fail to start: command = %v, state = %v", command, rf.state)
		return
	}
	rf.log = append(rf.log, &LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})
	index = len(rf.log)
	term = rf.currentTerm
	rf.logger.Printf("A log entry is appended: term = %v, index = %v, command = %v", term, index, command)
	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// WARNING: must hold mutex before calling this function!
func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(electionTimeoutMin+rand.Int()%(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond
	rf.nextDeadline = time.Now().Add(electionTimeout)
	rf.logger.Printf("Reset election timer: electionTimeout = %v, nextDeadline = %v", electionTimeout, rf.nextDeadline.Format("15:04:05.000"))
}

// WARNING: must hold mutex before calling this function!
func (rf *Raft) resetLeaderStates() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	rf.logger.Printf("Reset nextIndex and matchIndex: nextIndex[i] = %v, matchIndex[i] = %v", len(rf.log)+1, 0)
}

func (rf *Raft) sendAppendEntriesPeroidically() {
	rf.logger.Print("Start sendAppendEntriesPeroidically goroutine.")
	defer rf.logger.Print("Stop sendAppendEntriesPeroidically goroutine.")
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.leaderCond.L.Lock()
		// only send AppendEntries if it's leader
		for rf.state != Leader {
			rf.leaderCond.Wait()
		}
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		rf.logger.Printf("Ready to send heartbeats to all other servers: args = %v", args)
		for i := 0; i != len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := AppendEntriesReply{}
				attempts := 0
				for !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
					attempts++
				}
				rf.logger.Printf("Receive reply for AppendEntries RPC: peer = %v, reply = %v", server, reply)
				// TODO: more logic here
			}(i)
		}
		rf.leaderCond.L.Unlock()
	}
}

func (rf *Raft) checkLeaderPeriodically() {
	rf.logger.Print("Start checkLeaderPeriodically goroutine.")
	defer rf.logger.Print("Stop checkLeaderPeriodically goroutine.")
	for !rf.killed() {
		time.Sleep(checkPeriod)
		rf.mu.Lock()
		if rf.state == Leader || rf.nextDeadline.After(time.Now()) { // within election timeout
			rf.mu.Unlock()
			continue
		}
		// should kick off leader election
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = &rf.me // vote for myself
		rf.votes = 1         // count my own vote
		rf.logger.Printf("Kick off leader election: currentTerm = %v", rf.currentTerm)
		rf.resetElectionTimer()
		// send RequestVote RPCs to all other servers
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: len(rf.log),
			LastLogTerm:  0,
		}
		if len(rf.log) != 0 { // in case there's no log at all
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		}
		rf.logger.Printf("Ready to send RequestVote RPCs to all other servers: args = %v", args)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := RequestVoteReply{}
				attempts := 1
				for !rf.sendRequestVote(server, &args, &reply) {
					// repeat until success
					// rf.logger.Printf("Failed to get response for RequestVote, resend again: peer = %v", server)
					attempts++
				}
				rf.logger.Printf("Get reply for RequestVote RPC: peer = %v, attempts = %v, reply = %v", server, attempts, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Rule 2 for all servers in Figure 2
				if reply.Term > rf.currentTerm {
					rf.logger.Printf("Convert to follower: currentTerm = %v -> %v", rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = nil // reset votedFor as it's a new term
					rf.state = Follower
				}
				// it's important to check this vote is valid and NOT stale
				if reply.VoteGranted == true && rf.state == Candidate && rf.currentTerm == reply.Term {
					rf.votes++
					rf.logger.Printf("Receive a valid vote: peer = %v, currentTerm = %v", server, rf.currentTerm)
					if rf.votes > len(rf.peers)/2 { // collected votes from majority of servers, win!
						rf.logger.Printf("Win the election: votes = %v, peers = %v", rf.votes, len(rf.peers))
						rf.state = Leader
						rf.resetLeaderStates()
						rf.leaderCond.Broadcast() // wake up goroutine to send heartbeats
					}
				}
			}(i)
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:           sync.Mutex{},
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		state:        Follower,
		nextDeadline: time.Time{},
		votes:        0,
		logger:       log.New(os.Stdout, fmt.Sprintf("[Peer %v]", me), log.Ltime|log.Lmicroseconds),
		leaderCond:   nil,
		// Persistent state on all servers
		currentTerm: 0,
		votedFor:    nil,
		log:         nil,
		// Volatile state on all servers
		commitIndex: 0,
		lastApplied: 0,
		// Volatile state on leaders only
		nextIndex:  nil,
		matchIndex: nil,
	}
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()
	// Your initialization code here (2A, 2B, 2C).
	go rf.checkLeaderPeriodically()
	go rf.sendAppendEntriesPeroidically()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func init() {
	rand.Seed(time.Now().Unix())
}
