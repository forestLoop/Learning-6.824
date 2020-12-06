package raft

import (
	"math/rand"
	"time"
)

// resetElectionTimer resets the election timer with a random timeout interval between `electionTimeoutMin` and `electionTimeoutMax`
// WARNING: must hold mutex before calling this function!
func (rf *Raft) resetElectionTimer() {
	electionTimeout := time.Duration(electionTimeoutMin+rand.Int()%(electionTimeoutMax-electionTimeoutMin)) * time.Millisecond
	rf.nextDeadline = time.Now().Add(electionTimeout)
	rf.logger.Printf("Reset election timer: electionTimeout = %v, nextDeadline = %v", electionTimeout, rf.nextDeadline.Format("15:04:05.000"))
}

// resetLeaderStates reinitializes `nextIndex` and `matchIndex`
// WARNING: must hold mutex before calling this function!
func (rf *Raft) resetLeaderStates() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.logger.Printf("Reset nextIndex and matchIndex: nextIndex[i] = %v, matchIndex[i] = %v", len(rf.log), 0)
}
