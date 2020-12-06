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

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	return
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
	index = len(rf.log) - 1
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
	rf.logger.Print("Killed")
	muteLogger(rf.logger)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
		applyCond:    nil,
		applyCh:      applyCh,
		// Persistent state on all servers
		currentTerm: 0,
		votedFor:    -1,
		log:         []*LogEntry{{0, nil}}, // add one dummy log entry for simplicity in coding
		// Volatile state on all servers
		commitIndex: 0,
		lastApplied: 0,
		// Volatile state on leaders only
		nextIndex:  nil,
		matchIndex: nil,
	}
	muteLoggerIfUnset(rf.logger, "debug")
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()
	// Your initialization code here (2A, 2B, 2C).
	go rf.leaderElectionDaemon()
	go rf.logReplicationDaemon()
	go rf.commitIndexDaemon()
	go rf.applyMessagesDaemon()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func init() {
	rand.Seed(time.Now().Unix())
}
