package raft

import (
	"bytes"

	"github.com/keithnull/Learning-6.824/src/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.logger.Print("Start to persist data")
	defer rf.logger.Print("Finish persisting data")
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		rf.logger.Fatal("Failed to persist currentTerm:", err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		rf.logger.Fatal("Failed to persist votedFor:", err)
	}
	if err := e.Encode(rf.log); err != nil {
		rf.logger.Fatal("Failed to persist log:", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.logger.Print("Start to read data")
	defer rf.logger.Print("Finish reading data")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var log []*LogEntry
	if err := d.Decode(&currentTerm); err != nil {
		rf.logger.Fatal("Failed to read currentTerm:", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		rf.logger.Fatal("Failed to read votedFor:", err)
	}
	if err := d.Decode(&log); err != nil {
		rf.logger.Fatal("Failed to read log:", err)
	}
	rf.currentTerm, rf.votedFor, rf.log = currentTerm, votedFor, log
}
