package raft

import (
	"sort"
	"time"
)

// Warning: must hold lock when calling this function
func (rf *Raft) sendSnapshotTo(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	go func() {
		rf.logger.Printf("Send InstallSnapshot RPC to peer %v: args = %v", server, args)
		reply := InstallSnapshotReply{}
		attempts := 1
		for !rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply) {
			attempts++
			if attempts > maxAttempts {
				return
			}
		}
		rf.logger.Printf("Receive reply for InstallSnapshot RPC from peer %v: reply = %v, attempts = %v", server, reply, attempts)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.checkTerm(reply.Term)
		if rf.state != Leader || args.Term != rf.currentTerm {
			return // discard outdated reply
		}
		rf.nextIndex[server] = rf.getLastIndex() // jump directly to last index and step back in the future
		rf.matchIndex[server] = args.LastIncludedIndex
	}()
}

func (rf *Raft) sendEntriesTo(server int) {
	prevLogIndex := rf.nextIndex[server] - 1
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		LeaderCommit: rf.commitIndex,
	}
	if prevLogIndex == rf.lastIncludedIndex { // exactly the last log entry in snapshot
		rf.logger.Printf("Use the last included entry in snapshot: prevLogIndex = %v, rf.lastIncludedIndex = %v", prevLogIndex, rf.lastIncludedIndex)
		args.PrevLogTerm = rf.lastIncludedTerm
		args.Entries = rf.log
	} else { // currently available log entry
		prevLogPos := rf.index2pos(prevLogIndex)
		args.PrevLogTerm = rf.log[prevLogPos].Term
		args.Entries = rf.log[prevLogPos+1:]
	}
	go func() {
		rf.logger.Printf("Send AppendEntries RPC to peer %v: args = %v", server, args)
		reply := AppendEntriesReply{}
		attempts := 1
		for !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
			attempts++
			if attempts > maxAttempts {
				return
			}
		}
		rf.logger.Printf("Receive reply for AppendEntries RPC from peer %v: reply = %v, attempts = %v", server, reply, attempts)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.checkTerm(reply.Term)
		if rf.state != Leader || args.Term != rf.currentTerm || args.PrevLogIndex != rf.nextIndex[server]-1 {
			return // discard outdated reply
		}
		if reply.Success {
			newNextIndex := args.PrevLogIndex + 1 + len(args.Entries)
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			rf.logger.Printf("Success: nextIndex[%v] = %v -> %v, matchIndex[%v] = %v -> %v", server, rf.nextIndex[server], newNextIndex, server, rf.matchIndex[server], newMatchIndex)
			rf.nextIndex[server] = newNextIndex
			rf.matchIndex[server] = newMatchIndex
		} else {
			newNextIndex := reply.FirstIndex
			rf.logger.Printf("Failure: nextIndex[%v] = %v -> %v", server, rf.nextIndex[server], newNextIndex)
			rf.nextIndex[server] = newNextIndex
		}
	}()
}

func (rf *Raft) doLogReplication() {
	rf.logger.Print("Start to do log replication")
	defer rf.logger.Print("Finish doing log replication")
	for i := 0; i != len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[i] - 1
		if prevLogIndex < rf.lastIncludedIndex {
			rf.logger.Printf("The leader has discarded log entries needed, so send snapshot instead: prevLogIndex = %v, rf.lastIncludedIndex = %v", prevLogIndex, rf.lastIncludedIndex)
			rf.sendSnapshotTo(i)
		} else {
			rf.sendEntriesTo(i)
		}
	}
}

func (rf *Raft) logReplicationDaemon() {
	rf.logger.Print("Start logReplicationDaemon goroutine.")
	defer rf.logger.Print("Stop logReplicationDaemon goroutine.")
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.leaderCond.L.Lock()
		// only do log replication if it's leader
		for rf.state != Leader {
			rf.leaderCond.Wait()
		}
		rf.doLogReplication()
		rf.leaderCond.L.Unlock()
	}
}

func (rf *Raft) leaderElectionDaemon() {
	rf.logger.Print("Start leaderElectionDaemon goroutine.")
	defer rf.logger.Print("Stop leaderElectionDaemon goroutine.")
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
		rf.votedFor = rf.me // vote for myself
		rf.votes = 1        // count my own vote
		rf.persist()
		rf.logger.Printf("Kick off leader election: currentTerm = %v", rf.currentTerm)
		rf.resetElectionTimer()
		// send RequestVote RPCs to all other servers
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.getLastIndex(),
			LastLogTerm:  rf.getLastTerm(),
		}
		rf.logger.Printf("Ready to send RequestVote RPCs to all other servers: args = %v", args)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := RequestVoteReply{}
				attempts := 1
				for !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
					attempts++
					if attempts > maxAttempts {
						return
					}
				}
				rf.logger.Printf("Get reply for RequestVote RPC: peer = %v, attempts = %v, reply = %v", server, attempts, reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.checkTerm(reply.Term)
				// it's important to check this vote is valid and NOT stale
				if reply.VoteGranted && rf.state == Candidate && rf.currentTerm == reply.Term {
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

func (rf *Raft) commitIndexDaemon() {
	rf.logger.Print("Start commitIndexDaemon goroutine.")
	defer rf.logger.Print("Stop commitIndexDaemon goroutine.")
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.leaderCond.L.Lock()
		// only advance commitIndex if it's leader
		for rf.state != Leader {
			rf.leaderCond.Wait()
		}
		rf.logger.Printf("Check commitIndex: commitIndex = %v, matchIndex = %v", rf.commitIndex, rf.matchIndex)
		sortedMatchIndex := append([]int(nil), rf.matchIndex...)
		sort.Ints(sortedMatchIndex)
		newCommitIndex := sortedMatchIndex[len(rf.peers)/2+1]
		rf.logger.Printf("newCommitIndex = %v, rf.commitIndex = %v, rf.lastIncludedIndex = %v", newCommitIndex, rf.commitIndex, rf.lastIncludedIndex)
		for newCommitIndex > rf.commitIndex && rf.log[rf.index2pos(newCommitIndex)].Term != rf.currentTerm {
			newCommitIndex--
		}
		rf.tryUpdateCommitIndex(newCommitIndex)
		rf.leaderCond.L.Unlock()
	}
}

func (rf *Raft) applyMessagesDaemon() {
	rf.logger.Print("Start applyMessagesDaemon goroutine.")
	defer rf.logger.Print("Stop applyMessagesDaemon goroutine.")
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		rf.logger.Printf("Ready to apply messages: commitIndex = %v, lastApplied = %v, log = %v", rf.commitIndex, rf.lastApplied, rf.log)
		commitIndex := rf.commitIndex
		rf.applyCond.L.Unlock() // release lock here as applying messages may be blocking
		// no need to hold mutex for r/w lastApplied as only this goroutine would r/w it
		for rf.lastApplied < commitIndex {
			rf.lastApplied++
			// but it's necessary to hold mutex when reading rf.log
			rf.applyCond.L.Lock()
			pos := rf.index2pos(rf.lastApplied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[pos].Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.log[pos].Term,
			}
			rf.applyCond.L.Unlock()
			rf.logger.Printf("Apply message: msg = %v", msg)
			rf.applyCh <- msg
			rf.logger.Printf("Applied: msg = %v", msg)
		}
	}
}
