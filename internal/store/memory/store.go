package memory

import (
	"encoding/json"
	"log"
	"sync"

	"raft_kv/internal/consensus"
)

// Store 内存存储实现，支持从applyChan接收命令并应用到状态机
type Store struct {
	mu        sync.RWMutex
	data      map[string][]byte
	lastIndex int // 最后应用的日志索引
	lastTerm  int // 最后应用的日志任期
}

// Command 表示要应用到状态机的命令
type Command struct {
	Op    string `json:"op"`    // 操作类型：put、get、delete
	Key   string `json:"key"`   // 操作的键
	Value []byte `json:"value"` // 操作的值（仅用于put）
}

// NewStore 创建一个新的内存存储实例
func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
	}
}

// Start 启动存储服务，开始处理从applyChan来的消息
func (s *Store) Start(applyChan <-chan *consensus.ApplyMsg) {
	go s.processApplyMsg(applyChan)
}

// processApplyMsg 处理从applyChan来的消息
func (s *Store) processApplyMsg(applyChan <-chan *consensus.ApplyMsg) {
	for msg := range applyChan {
		if msg.SnapshotValid {
			// 处理快照
			s.handleSnapshot(msg)
		} else if msg.CommandValid {
			// 处理命令
			s.handleCommand(msg)
		}
	}
}

// handleSnapshot 处理快照消息
func (s *Store) handleSnapshot(msg *consensus.ApplyMsg) {
	// 解析快照数据
	var snapshotData map[string][]byte
	if err := json.Unmarshal(msg.Snapshot, &snapshotData); err != nil {
		log.Printf("Error unmarshaling snapshot: %v", err)
		return
	}

	// 锁定存储，替换数据
	s.mu.Lock()
	defer s.mu.Unlock()

	// 替换整个数据映射
	s.data = snapshotData
	// 更新最后应用的索引和任期
	s.lastIndex = msg.SnapshotIndex
	s.lastTerm = msg.SnapshotTerm

	log.Printf("Applied snapshot with index %d, term %d", msg.SnapshotIndex, msg.SnapshotTerm)
}

// handleCommand 处理命令消息
func (s *Store) handleCommand(msg *consensus.ApplyMsg) {
	// 解析命令数据
	cmdBytes, ok := msg.Command.([]byte)
	if !ok {
		log.Printf("Invalid command type: %T", msg.Command)
		return
	}

	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		log.Printf("Error unmarshaling command: %v", err)
		return
	}

	// 锁定存储，执行命令
	s.mu.Lock()
	defer s.mu.Unlock()

	// 根据命令类型执行操作
	switch cmd.Op {
	case "put":
		if err := s.Put(cmd.Key, cmd.Value); err != nil {
			log.Printf("Error putting key: %v", err)
			return
		}

	case "delete":
		if err := s.Delete(cmd.Key); err != nil {
			log.Printf("Error deleting key: %v", err)
			return
		}
		// get 操作不需要在状态机中执行，因为它只是读取数据
	}

	// 更新最后应用的索引
	s.lastIndex = msg.CommandIndex

	log.Printf("Applied command %s at index %d", cmd.Op, msg.CommandIndex)
}

// Put 将键值对写入存储
func (s *Store) Put(key string, value []byte) error {
	s.data[key] = value
	return nil
}

// Get 从存储中读取键值对
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

// Delete 从存储中删除键值对
func (s *Store) Delete(key string) error {
	delete(s.data, key)
	return nil
}

// CreateSnapshot 创建存储的快照
func (s *Store) CreateSnapshot(lastIncludedIndex, lastIncludedTerm int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 将数据序列化为JSON
	snapshotData, err := json.Marshal(s.data)
	if err != nil {
		return nil, err
	}

	log.Printf("Created snapshot with index %d, term %d", lastIncludedIndex, lastIncludedTerm)
	return snapshotData, nil
}

// GetLastApplied 获取最后应用的日志索引和任期
func (s *Store) GetLastApplied() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex, s.lastTerm
}
