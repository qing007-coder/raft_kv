package consensus

import (
	pb "raft_kv/internal/consensus/proto"
	"sync"
)

type LogManager struct {
	mu        sync.RWMutex
	persister *Persister // 用于将日志实时落盘

	// 内存中的日志缓存
	entries []LogEntry

	// 快照偏移 Index可能不等于0了
	lastIncludedIndex int64 // 快照中包含的最后一条日志索引
	lastIncludedTerm  int64 // 快照中包含的最后一条日志任期
}

// LogEntry 保持简洁，以便序列化
type LogEntry struct {
	Index   int64
	Term    int64
	Command []byte // 建议工业级使用 []byte，方便存储
}

func NewLogManager() *LogManager {
	return &LogManager{
		persister:         nil,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		entries:           make([]LogEntry, 0),
	}
}

// 1. 获取物理下标：逻辑 Index 转为 entries 数组的 Index
func (logMgr *LogManager) getPhysicalIndex(logicalIndex int64) int {
	return int(logicalIndex - logMgr.lastIncludedIndex - 1)
}

// GetLastLogInfo 获取最后一条日志的元数据 (包含快照情况)
func (logMgr *LogManager) GetLastLogInfo() (index int64, term int64) {
	logMgr.mu.RLock()
	defer logMgr.mu.RUnlock()

	if len(logMgr.entries) == 0 {
		return logMgr.lastIncludedIndex, logMgr.lastIncludedTerm
	}
	last := logMgr.entries[len(logMgr.entries)-1]
	return last.Index, last.Term
}

// GetConflictInfo 快速回溯逻辑：找到冲突 Term 的第一条日志索引
func (logMgr *LogManager) GetConflictInfo(prevIndex int64) (conflictIndex int64, conflictTerm int64) {
	logMgr.mu.RLock()
	defer logMgr.mu.RUnlock()

	lastIndex, _ := logMgr.GetLastLogInfo()

	// 情况 1: 我太短了，根本没到 prevIndex
	if prevIndex > lastIndex {
		return lastIndex + 1, -1
	}

	// 情况 2: 我有这条日志，但 Term 不对
	pIdx := logMgr.getPhysicalIndex(prevIndex)
	if pIdx < 0 { // 已经进快照了
		return logMgr.lastIncludedIndex, logMgr.lastIncludedTerm
	}

	conflictTerm = logMgr.entries[pIdx].Term
	// 往前找，找到该 Term 的第一条日志
	for i := pIdx; i >= 0; i-- {
		if logMgr.entries[i].Term != conflictTerm {
			break
		}
		conflictIndex = logMgr.entries[i].Index
	}
	return conflictIndex, conflictTerm
}

// checkMatchInternal 底层通用的比对逻辑
func (logMgr *LogManager) checkMatchInternal(index int64, term int64) bool {
	if index == logMgr.lastIncludedIndex {
		return term == logMgr.lastIncludedTerm
	}
	if index < logMgr.lastIncludedIndex {
		return true
	}
	pIdx := logMgr.getPhysicalIndex(index)
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return false
	}
	return logMgr.entries[pIdx].Term == term
}

// MatchLog 对账检查：Leader PrevLogIndex/Term 是否匹配
func (logMgr *LogManager) MatchLog(index int64, term int64) bool {
	return logMgr.checkMatchInternal(index, term)
}

// IsEntryMatch 检查本地指定索引处的日志 Term 是否匹配
func (logMgr *LogManager) IsEntryMatch(index int64, term int64) bool {
	return logMgr.checkMatchInternal(index, term)
}

// TruncateFrom 丢弃从指定 index 开始（含）之后的所有日志
func (logMgr *LogManager) TruncateFrom(index int64) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	pIdx := logMgr.getPhysicalIndex(index)
	// 如果截断点已经在快照里了，或者越界了，不执行操作
	if pIdx < 0 || pIdx >= len(logMgr.entries) {
		return
	}

	// 真正的截断操作：只保留 0 到 pIdx 之前的部分
	logMgr.entries = logMgr.entries[:pIdx]

	// 工业级关键：截断也是一种状态改变，必须持久化，否则重启后脏数据又回来了
	//logMgr.persist()
}

// Append 将一组新日志追加到当前日志末尾
func (logMgr *LogManager) Append(newEntries []*pb.LogEntry) {
	if len(newEntries) == 0 {
		return
	}

	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	// 将 protobuf 格式转为本地格式并追加
	for _, e := range newEntries {
		logMgr.entries = append(logMgr.entries, LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Data,
		})
	}

	// 写入磁盘
	//logMgr.persist()
}

func (logMgr *LogManager) TruncateBefore(lastIndex int64, lastTerm int64) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	// 1. 找到还需保留的日志起点
	// 比如我们要把 Index 100 之前的都删了，101 及以后的保留
	keepFromIdx := logMgr.getPhysicalIndex(lastIndex + 1)

	if keepFromIdx < 0 {
		// 说明要删的比我现有的还多，直接全清空
		logMgr.entries = make([]LogEntry, 0)
	} else {
		// 核心：创建一个新切片并拷贝，断开对老数组的引用
		newEntries := make([]LogEntry, len(logMgr.entries)-keepFromIdx)
		copy(newEntries, logMgr.entries[keepFromIdx:])
		logMgr.entries = newEntries
	}

	// 2. 更新元数据
	logMgr.lastIncludedIndex = lastIndex
	logMgr.lastIncludedTerm = lastTerm
}

// ResetWithSnapshot 清空所有日志，并以快照元数据作为新的起点
func (logMgr *LogManager) ResetWithSnapshot(lastIndex int64, lastTerm int64) {
	logMgr.mu.Lock()
	defer logMgr.mu.Unlock()

	// 1. 清空内存日志数组
	// 工业级建议：直接指向一个空切片，让老数组被 GC 回收
	logMgr.entries = make([]LogEntry, 0)

	// 2. 更新快照元数据
	logMgr.lastIncludedIndex = lastIndex
	logMgr.lastIncludedTerm = lastTerm

	// 注意：这里不需要手动写盘，因为 Raft 层的 InstallSnapshot
	// 会在调用完这个函数后，统一调用 persister.SaveStateAndSnapshot
}
