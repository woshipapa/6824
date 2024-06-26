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
		return rf.getEntryTerm(rf.Log.LastLogIndex)
	}
	return -1
}
func NewLog() *Log {
	return &Log{
		Entries:       make([]Entry, 0),
		LastLogIndex:  0, //之前从0开始的话这里是 -1
		FirstLogIndex: 1, //这里是0
	}
}
func (log *Log) getRealIndex(index int) int {
	return index - log.FirstLogIndex
}

func (log *Log) getOneEntry(index int) *Entry {

	return &log.Entries[log.getRealIndex(index)]
}
func (rf *Raft) getEntryTerm(index int) int {
	if index == 0 {
		return 0
	}
	if index == rf.Log.FirstLogIndex-1 {
		return rf.snapshotLastIncludeTerm
	}
	if rf.Log.FirstLogIndex <= rf.Log.LastLogIndex {
		return rf.Log.getOneEntry(index).Term
	}

	DPrintf("invalid index=%v in getEntryTerm rf.log.FirstLogIndex=%v rf.log.LastLogIndex=%v\n", index, rf.Log.FirstLogIndex, rf.Log.LastLogIndex)
	return -1
}
func (log *Log) appendL(newEntries ...Entry) {
	//这里会截断后面不匹配的
	log.Entries = append(log.Entries[:log.getRealIndex(log.LastLogIndex)+1], newEntries...)
	log.LastLogIndex += len(newEntries)

}
func (log *Log) empty() bool {
	return log.FirstLogIndex > log.LastLogIndex
}
