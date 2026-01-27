package consensus

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
)

type Persister struct {
	mu        sync.RWMutex
	raftState []byte
	snapshot  []byte

	// 存储路径
	statePath string
	snapPath  string
}

func NewPersister(basePath string) *Persister {
	p := &Persister{
		statePath: basePath + ".state",
		snapPath:  basePath + ".snap",
	}
	p.readFromDisk()
	return p
}

// SaveStateAndSnapshot 同时持久化 Raft 状态和快照 保证这两个操作的原子性或一致性
func (p *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.raftState = state
	p.snapshot = snapshot

	// 原子写入 Raft 状态
	p.atomicWrite(p.statePath, state)

	// 原子写入快照
	if len(snapshot) > 0 {
		p.atomicWrite(p.snapPath, snapshot)
	}
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.raftState
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snapshot
}

func (p *Persister) RaftStateSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.raftState)
}

// atomicWrite 工业级核心：先写临时文件，Sync 确保落盘，重命名原子覆盖
func (p *Persister) atomicWrite(path string, data []byte) {
	tmpPath := path + ".tmp"

	// WriteFile 内部会创建文件，但我们手动处理以确保 Sync
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Persister Error: create tmp file failed: %v\n", err)
		return
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return
	}

	// 关键：强制操作系统刷盘，防止断电导致文件空洞
	if err := f.Sync(); err != nil {
		f.Close()
		return
	}
	f.Close()

	// 关键：利用 OS 系统调用的原子性替换旧文件
	if err := os.Rename(tmpPath, path); err != nil {
		fmt.Printf("Persister Error: rename failed: %v\n", err)
	}
}

func (p *Persister) readFromDisk() {
	// 读取 Raft 状态
	if state, err := ioutil.ReadFile(p.statePath); err == nil {
		p.raftState = state
	}
	// 读取快照
	if snap, err := ioutil.ReadFile(p.snapPath); err == nil {
		p.snapshot = snap
	}
}
