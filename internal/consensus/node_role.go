package consensus

// NodeRole 角色枚举
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)
