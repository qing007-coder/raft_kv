package store

import "raft_kv/internal/consensus"

type Store interface {
	// 基本操作
	Put(key string, value []byte) error
	Get(key string) ([]byte, bool)
	Delete(key string) error

	// 状态机操作
	Start(applyChan <-chan *consensus.ApplyMsg)
	CreateSnapshot(lastIncludedIndex, lastIncludedTerm int) ([]byte, error)
	GetLastApplied() (int, int)
}
