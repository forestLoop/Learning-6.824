package raft

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

// WARNING: must hold mutex before calling this function!
// resetElectionTimer resets the election timer with a random timeout interval between `electionTimeoutMin` and `electionTimeoutMax`
func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(electionTimeoutMin+rand.Int()%(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond
	rf.nextDeadline = time.Now().Add(electionTimeout)
	rf.logger.Printf("Reset election timer: electionTimeout = %v, nextDeadline = %v", electionTimeout, rf.nextDeadline.Format("15:04:05.000"))
}

// WARNING: must hold mutex before calling this function!
// resetLeaderStates reinitializes `nextIndex` and `matchIndex`
func (rf *Raft) resetLeaderStates() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.logger.Printf("Reset nextIndex and matchIndex: nextIndex[i] = %v, matchIndex[i] = %v", len(rf.log), 0)
}

// WARNING: must hold mutex before calling this function!
// tryUpdateCommitIndex updates commitIndex only if newCommitIndex is larger
// if updated, it also notifies other goroutines waiting `applyCond`
func (rf *Raft) tryUpdateCommitIndex(newCommitIndex int) {
	if rf.commitIndex >= newCommitIndex {
		return
	}
	rf.logger.Printf("Update commitIndex: %v -> %v", rf.commitIndex, newCommitIndex)
	rf.commitIndex = newCommitIndex
	// notify another goroutine to apply more commands
	rf.applyCond.Broadcast()
}

// WARNING: must hold mutex before calling this function!
// Rule 2 for all servers in Figure 2
func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.logger.Printf("Convert to follower: currentTerm = %v -> %v", rf.currentTerm, term)
		rf.currentTerm = term
		rf.votedFor = nil // reset votedFor as it's a new term
		rf.state = Follower
	}
}

func muteLoggerIfUnset(logger *log.Logger, env string) {
	if os.Getenv(env) == "" {
		muteLogger(logger)
	}
}

func muteLogger(logger *log.Logger) {
	logger.SetOutput(ioutil.Discard)
}
