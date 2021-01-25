package raft

// This file includes RPC handlers, their argument and reply type definitions

// --- RequestVote RPC ---

// RequestVoteArgs is the RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply is the RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // receiver's current term
	VoteGranted bool // whether receiver granted its vote to the candidate
}

// RequestVote is the RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start to handle incoming RequestVote RPC: args = %v", args)
	defer rf.logger.Printf("Finish handling incoming RequestVote RPC: reply = %v", reply)
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false       // by default, deny the request
	if args.Term < rf.currentTerm { // candidate's term is older than receiver
		rf.logger.Printf("args.Term (%v) < currentTerm (%v)", args.Term, rf.currentTerm)
		return
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateID { //  receiver has already voted for another candidate
		rf.logger.Printf("Already voted for another candidate: votedFor = %v, candidateID = %v", rf.votedFor, args.CandidateID)
		return
	}
	// 1. receiver's last log term is older than candidate's last log term
	isLogUpToDate := rf.getLastTerm() < args.LastLogTerm
	// 2. receiver's last log term is the same as candidate's last log term, but its log is shorter
	isLogUpToDate = isLogUpToDate || (rf.getLastTerm() == args.LastLogTerm && rf.getLastIndex() <= args.LastLogIndex)
	rf.logger.Printf("Candidate's log is at least as up-to-date as my log? %v", isLogUpToDate)
	reply.VoteGranted = isLogUpToDate // grant vote if candidate's log is at least as up-to-date as receiver's log
	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
	}
}

// --- AppendEntries RPC ---

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
	Term            int  // receiver's term, for leader to update itself
	Success         bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictingTerm int  // if mismatched, the term of the conflicting entry
	FirstIndex      int  // if mismatched, the first index it stores for that term
}

// AppendEntries is the AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start to handle incoming AppendEntries RPC: args = %v", args)
	defer rf.logger.Printf("Finish handling incoming AppendEntries RPC: reply = %v", reply)
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.logger.Printf("args.Term (%v) < currentTerm (%v)", args.Term, rf.currentTerm)
		return
	}
	// till now, we are sure this RPC is from the current leader
	rf.resetElectionTimer()
	// reply false if log doesn’t contain an entry at PrevLogIndex whose term matches PrevLogTerm
	matched := (rf.isValidIndex(args.PrevLogIndex) && rf.getEntry(args.PrevLogIndex).Term == args.PrevLogTerm)
	// special case: prevLogIndex is exactly the last included index in snapshot
	matched = matched || (rf.lastIncludedIndex == args.PrevLogIndex && rf.lastIncludedTerm == args.PrevLogTerm)
	if !matched {
		var entry *LogEntry = nil
		pos := rf.index2pos(args.PrevLogIndex)
		if pos >= len(rf.log) { // index out of range
			reply.ConflictingTerm = -1
			reply.FirstIndex = rf.getLastIndex() + 1
		} else if pos < 0 { // index is inside the snapshot
			reply.ConflictingTerm = -1
			reply.FirstIndex = 1 // require an InstallSnapshot
		} else { // simply mismatch
			entry = rf.log[pos]
			reply.ConflictingTerm = entry.Term
			for pos >= 0 && rf.log[pos].Term == reply.ConflictingTerm {
				pos--
			}
			if pos < 0 {
				reply.FirstIndex = 1
			} else {
				reply.FirstIndex = rf.pos2index(pos + 1)
			}
		}
		rf.logger.Printf("Log doesn't contain a matching entry at PrevLogIndex: PrevLogTerm = %v, log entry = %v", args.PrevLogTerm, entry)
		rf.logger.Printf("Fast stepback: ConflictingTerm = %v, FirstIndex = %v, pos = %v, log = %v", reply.ConflictingTerm, reply.FirstIndex, rf.index2pos(reply.FirstIndex), rf.log)
		return
	}
	// add these entries
	oldLog := rf.log
	for i, entry := range args.Entries {
		pos := rf.index2pos(args.PrevLogIndex + i + 1)
		if len(rf.log) <= pos { // out of existing log entries, simply append it
			rf.log = append(rf.log, entry)
		} else { // still within existing log entries
			if entry.Term != rf.log[pos].Term { // conflicting entry
				rf.log = rf.log[:pos] // delete the existing entry and all that follow it
				rf.log = append(rf.log, entry)
			}
			// otherwise, do nothing to this entry
		}
	}
	rf.persist()
	rf.logger.Printf("Log entries: %v -> %v", oldLog, rf.log)
	if args.LeaderCommit > rf.commitIndex {
		// take the min of LeaderCommit and index of last new entry
		newCommitIndex := args.PrevLogIndex + len(args.Entries)
		if newCommitIndex > args.LeaderCommit {
			newCommitIndex = args.LeaderCommit
		}
		rf.tryUpdateCommitIndex(newCommitIndex)
	}
	reply.Success = true
}

// --- InstallSnapshot RPC ---

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so followers can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // actual data for snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("Start to handle incoming InstallSnapshot RPC: args = %v", args)
	rf.logger.Printf("rf.log = %v, rf.lastIncludedIndex = %v", rf.log, rf.lastIncludedIndex)
	defer rf.logger.Printf("Finish handling incoming InstallSnapshot RPC: reply = %v", reply)
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.logger.Printf("args.Term (%v) < currentTerm (%v)", args.Term, rf.currentTerm)
		return
	}
	// till now, we are sure this RPC is from the current leader
	rf.resetElectionTimer()
	if args.LastIncludedIndex <= rf.lastIncludedIndex { // this snapshot is stale
		rf.logger.Printf("The snapshot is stale: args.LastIncludedIndex = %v, rf.lastIncludedIndex = %v", args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	// existing log entry has same index and term as snapshot’s last included entry
	// retain log entries following it and reply
	if rf.isValidIndex(args.LastIncludedIndex) && rf.getEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.logger.Printf("Matched snapshot, retain remaining log entries: args.LastIncludedIndex = %v, args.LastIncludedTerm = %v, entry = %v", args.LastIncludedIndex, args.LastIncludedTerm, rf.getEntry(args.LastIncludedIndex))
		rf.discardEntriesBefore(args.LastIncludedIndex, false)
		if rf.commitIndex < args.LastIncludedIndex {
			// in case commitIndex is larger, in which situation we then need to apply following entries
			rf.commitIndex = args.LastIncludedIndex
		}
	} else {
		rf.logger.Printf("Discard the entire log")
		rf.discardEntriesBefore(rf.getLastIndex(), false)
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.commitIndex = args.LastIncludedIndex
	}
	// set lastApplied
	rf.lastApplied = args.LastIncludedIndex
	// persist with snapshot
	rf.persistWithSnapshot(args.Data)
	// reset state machine using this snapshot content
	rf.initSnapshot(args.Data)
}
