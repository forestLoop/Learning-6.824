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
		rf.votedFor = -1 // reset votedFor as it's a new term
		rf.state = Follower
		rf.persist()
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

// --- Log Compaction Related Utilities ---
// WARNING: must hold mutex before calling all following functions!

func (rf *Raft) getLastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

func (rf *Raft) getLastTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) index2pos(index int) int {
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) pos2index(pos int) int {
	return pos + 1 + rf.lastIncludedIndex
}

func (rf *Raft) isValidIndex(index int) bool {
	pos := rf.index2pos(index)
	return 0 <= pos && pos < len(rf.log)
}

func (rf *Raft) getEntry(index int) *LogEntry {
	return rf.log[rf.index2pos(index)]
}

// discardEntriesBefore discards all log entries before `index` (inclusive)
// It's expected that rf.lastIncludedIndex < index <= rf.lastApplied
func (rf *Raft) discardEntriesBefore(index int) {
	rf.logger.Printf("Start to discard entries: index = %v, lastIncludedIndex = %v", index, rf.lastIncludedIndex)
	if index > rf.getLastIndex() { // sanity check: you can at most discard all entries
		index = rf.getLastIndex()
	}
	if index <= rf.lastIncludedIndex {
		rf.logger.Printf("No need to discard entries")
	} else if index > rf.lastApplied {
		rf.logger.Fatalf("You should not discard unapplied entries: index = %v, rf.lastApplied = %v", index, rf.lastApplied)
	} else {
		// set raft status correctly
		rf.lastIncludedTerm = rf.getEntry(index).Term
		rf.lastIncludedIndex = index
		// do the truncation actually
		oldLog := rf.log
		rf.log = rf.log[rf.index2pos(index)+1:]
		rf.logger.Printf("Entries discarded: %v -> %v", oldLog, rf.log)
		// no need to persist here as the caller will handle it
	}
}
