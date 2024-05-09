package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Entries       []Entry
	LastLogIndex  int
	FirstLogIndex int
}

func (rf *Raft) getLastEntryTerm() int {
	if rf.Log.LastLogIndex >= rf.Log.FirstLogIndex {
		return rf.Log.Entries[rf.Log.LastLogIndex].Term
	}
	return -1
}
func NewLog() *Log {
	return &Log{
		Entries:       make([]Entry, 0),
		LastLogIndex:  0,
		FirstLogIndex: 1,
	}
}
