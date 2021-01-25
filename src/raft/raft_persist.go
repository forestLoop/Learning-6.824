package raft

import (
	"bytes"

	"github.com/keithnull/Learning-6.824/src/labgob"
)

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// I know it's bad to ignore error checking, but just for simplicity :)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.lastIncludedIndex)
	_ = e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.logger.Print("Start to persist data")
	defer rf.logger.Print("Finish persisting data")
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.logger.Print("Start to persist data with snapshot")
	defer rf.logger.Print("Finish persisting data with snapshot")
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
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
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
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
	if err := d.Decode(&lastIncludedIndex); err != nil {
		rf.logger.Fatal("Failed to read lastIncludedIndex:", err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		rf.logger.Fatal("Failed to read lastIncludedTerm:", err)
	}
	rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log = currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm, log
	if rf.lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludedIndex
	}
}
