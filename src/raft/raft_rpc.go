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
	isLogUpToDate := rf.log[len(rf.log)-1].Term < args.LastLogTerm
	// 2. receiver's last log term is the same as candidate's last log term, but its log is shorter
	isLogUpToDate = isLogUpToDate || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) <= args.LastLogIndex+1)
	rf.logger.Printf("Candidate's log is at least as up-to-date as my log? %v", isLogUpToDate)
	reply.VoteGranted = isLogUpToDate // grant vote if candidate's log is at least as up-to-date as receiver's log
	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
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
	matched := (0 <= args.PrevLogIndex && args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)
	if !matched {
		var entry *LogEntry = nil
		if args.PrevLogIndex < 0 { // would this be possible?
			reply.ConflictingTerm = 0
			reply.FirstIndex = 0
			rf.logger.Fatalf("Oops: PrevLogIndex = %v, PrevLogTerm = %v, log = %v", args.PrevLogIndex, args.PrevLogTerm, rf.log)
		} else if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictingTerm = -1
			reply.FirstIndex = len(rf.log)
		} else {
			entry = rf.log[args.PrevLogIndex]
			reply.ConflictingTerm = entry.Term
			reply.FirstIndex = args.PrevLogIndex
			for reply.FirstIndex >= 0 && rf.log[reply.FirstIndex].Term == reply.ConflictingTerm {
				reply.FirstIndex--
			}
			reply.FirstIndex++
		}
		rf.logger.Printf("Log doesn't contain a matching entry at PrevLogIndex: PrevLogTerm = %v, log entry = %v", args.PrevLogTerm, entry)
		rf.logger.Printf("Fast stepback: ConflictingTerm = %v, FirstIndex = %v, log = %v", reply.ConflictingTerm, reply.FirstIndex, rf.log)
		return
	}
	// add these entries
	oldLog := rf.log
	for i, entry := range args.Entries {
		if len(rf.log) <= i+args.PrevLogIndex+1 { // out of existing log entries, simply append it
			rf.log = append(rf.log, entry)
		} else { // still within existing log entries
			if entry.Term != rf.log[args.PrevLogIndex+i+1].Term { // conflicting entry
				rf.log = rf.log[:args.PrevLogIndex+i+1] // delete the existing entry and all that follow it
				rf.log = append(rf.log, entry)
			}
			// otherwise, do nothing to this entry
		}
	}
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
