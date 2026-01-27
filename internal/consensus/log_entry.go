package consensus

import (
	pb "raft_kv/internal/consensus/proto"
)

// LogEntry 保持简洁，以便序列化
type LogEntry struct {
	Index   int64
	Term    int64
	Command []byte
}

type LogManager struct {
	// 内存中的日志缓存
	entries []LogEntry

	// 快照元数据
	lastIncludedIndex int64 // 快照中包含的最后一条日志索引
	lastIncludedTerm  int64 // 快照中包含的最后一条日志任期
}

func NewLogManager() *LogManager {
	return &LogManager{
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		entries:           make([]LogEntry, 0),
	}
}

// 1. 获取物理下标：逻辑 Index 转为 entries 数组的 Index
// 如果返回负数，说明该索引已经在快照中
func (logMgr *LogManager) getPhysicalIndex(logicalIndex int64) int {
	return int(logicalIndex - logMgr.lastIncludedIndex - 1)
}

// GetLastLogInfo 获取最后一条日志的元数据 (包含快照情况)
func (logMgr *LogManager) GetLastLogInfo() (index int64, term int64) {
	if len(logMgr.entries) == 0 {
		return logMgr.lastIncludedIndex, logMgr.lastIncludedTerm
	}
	last := logMgr.entries[len(logMgr.entries)-1]
	return last.Index, last.Term
}

// GetConflictInfo 快速回溯逻辑：找到冲突 Term 的第一条日志索引
func (logMgr *LogManager) GetConflictInfo(prevIndex int64) (conflictIndex int64, conflictTerm int64) {
	lastIndex, _ := logMgr.GetLastLogInfo()

	// 1. 如果日志太短，根本没到 prevIndex
	if prevIndex > lastIndex {
		return lastIndex + 1, -1
	}

	// 2. 如果 prevIndex 已经在快照里了
	pIdx := logMgr.getPhysicalIndex(prevIndex)
	if pIdx < 0 {
		return logMgr.lastIncludedIndex, logMgr.lastIncludedTerm
	}

	// 3. 找到冲突 Term 的第一条日志索引
	conflictTerm = logMgr.entries[pIdx].Term
	conflictIndex = prevIndex
	for i := pIdx; i >= 0; i-- {
		if logMgr.entries[i].Term != conflictTerm {
			break
		}
		conflictIndex = logMgr.entries[i].Index
	}
	return conflictIndex, conflictTerm
}

// MatchLog 对账检查：Leader PrevLogIndex/Term 是否匹配
func (logMgr *LogManager) MatchLog(index int64, term int64) bool {
	if index == logMgr.lastIncludedIndex {
		return term == logMgr.lastIncludedTerm
	}
	// 即使 index 小于快照索引，我们也认为它“匹配”，因为那部分已经提交了
	if index < logMgr.lastIncludedIndex {
		return true
	}
	pIdx := logMgr.getPhysicalIndex(index)
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return false
	}
	return logMgr.entries[pIdx].Term == term
}

// IsEntryMatch 检查本地指定索引处的日志 Term 是否匹配
func (logMgr *LogManager) IsEntryMatch(index int64, term int64) bool {
	pIdx := logMgr.getPhysicalIndex(index)
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		// 如果在快照里，则比对快照元数据
		if index == logMgr.lastIncludedIndex {
			return term == logMgr.lastIncludedTerm
		}
		return false
	}
	return logMgr.entries[pIdx].Term == term
}

// TruncateFrom 丢弃从指定 index 开始（含）之后的所有日志
func (logMgr *LogManager) TruncateFrom(index int64) {
	pIdx := logMgr.getPhysicalIndex(index)
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return
	}
	// 截断操作：只保留 0 到 pIdx 之前的部分
	logMgr.entries = logMgr.entries[:pIdx]
}

// Append 将一组新日志追加到当前日志末尾
func (logMgr *LogManager) Append(newEntries []*pb.LogEntry) {
	if len(newEntries) == 0 {
		return
	}
	for _, e := range newEntries {
		logMgr.entries = append(logMgr.entries, LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Data,
		})
	}
}

// TruncateBefore 当完成快照后，删除 lastIndex 及其之前的日志
func (logMgr *LogManager) TruncateBefore(lastIndex int64, lastTerm int64) {
	keepFromIdx := logMgr.getPhysicalIndex(lastIndex + 1)

	if keepFromIdx <= 0 {
		// 如果要删的比现有的还多，全清空
		logMgr.entries = make([]LogEntry, 0)
	} else {
		// 工业级关键：创建新切片并拷贝，确保老数组被 GC
		newEntries := make([]LogEntry, len(logMgr.entries)-keepFromIdx)
		copy(newEntries, logMgr.entries[keepFromIdx:])
		logMgr.entries = newEntries
	}

	logMgr.lastIncludedIndex = lastIndex
	logMgr.lastIncludedTerm = lastTerm
}

// ResetWithSnapshot 清空所有日志，并以快照元数据作为新的起点
func (logMgr *LogManager) ResetWithSnapshot(lastIndex int64, lastTerm int64) {
	logMgr.entries = make([]LogEntry, 0)
	logMgr.lastIncludedIndex = lastIndex
	logMgr.lastIncludedTerm = lastTerm
}

// GetTermAtIndex 获取指定索引处的 Term
func (logMgr *LogManager) GetTermAtIndex(index int64) int64 {
	if index == logMgr.lastIncludedIndex {
		return logMgr.lastIncludedTerm
	}
	pIdx := logMgr.getPhysicalIndex(index)
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return -1
	}
	return logMgr.entries[pIdx].Term
}

// GetEntriesFrom 获取从 nextIndex 开始的所有日志（用于 Leader 复制）
func (logMgr *LogManager) GetEntriesFrom(nextIndex int64) []*pb.LogEntry {
	pIdx := logMgr.getPhysicalIndex(nextIndex)

	// 如果 pIdx < 0，调用方（Raft）应该意识到需要发送快照了
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return nil
	}

	size := len(logMgr.entries) - pIdx
	entries := make([]*pb.LogEntry, size)
	// 实现拷贝逻辑
	for i := 0; i < size; i++ {
		src := logMgr.entries[pIdx+i]
		entries[i] = &pb.LogEntry{
			Index: src.Index,
			Term:  src.Term,
			Data:  src.Command,
		}
	}
	return entries
}
