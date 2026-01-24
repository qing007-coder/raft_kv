package consensus

// ApplyMsg 提交给状态机的信息
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool   // 如果是快照，为 true
	Snapshot      []byte // 快照的原始二进制数据
	SnapshotTerm  int    // 快照包含的最后一条日志的任期
	SnapshotIndex int    // 快照包含的最后一条日志的索引
}
