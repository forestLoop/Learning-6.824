package raft

import (
	"sort"
	"time"
)

func (rf *Raft) logReplicationDaemon() {
	rf.logger.Print("Start logReplicationDaemon goroutine.")
	defer rf.logger.Print("Stop logReplicationDaemon goroutine.")
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.leaderCond.L.Lock()
		// only send AppendEntries if it's leader
		for rf.state != Leader {
			rf.leaderCond.Wait()
		}
		for i := 0; i != len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      rf.log[prevLogIndex+1:],
			}
			go func(server int, args AppendEntriesArgs) {
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
			}(i, args)
		}
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
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
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
		for newCommitIndex > rf.commitIndex && rf.log[newCommitIndex].Term != rf.currentTerm {
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
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCond.L.Unlock()
			rf.logger.Printf("Apply message: msg = %v", msg)
			rf.applyCh <- msg
			rf.logger.Printf("Applied: msg = %v", msg)
		}
	}
}
