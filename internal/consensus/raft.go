package consensus

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "raft_kv/internal/consensus/proto"
	"raft_kv/internal/tools"
	"sync"
	"time"
)

type Raft struct {
	mu        sync.RWMutex
	ctx       context.Context
	peersConn []pb.RaftInternalClient
	peers     []Node
	nodeID    string
	dead      int32

	// --- 持久化状态 ---
	persister   *Persister
	currentTerm int64
	votedFor    string
	logMgr      *LogManager // 引入独立的日志管理器

	// --- 内存状态 ---
	commitIndex int64 // 改为 int64，与 LogEntry.Index 保持一致
	lastApplied int64
	role        NodeRole

	// --- 状态通知 ---
	applyCh   chan ApplyMsg // 放在 Raft 层，负责向上层应用推送数据
	applyCond *sync.Cond    // 用于唤醒 applyLoop 协程

	// --- 计时器 ---
	electionTimer     *time.Timer
	heartbeatTimer    *time.Timer
	heartbeatDuration time.Duration
	voteCh            chan *pb.RequestVoteReply
	votesReceived     map[string]bool
	// --- Leader 独有状态 ---
	// 工业级实现中，建议用 map[string]int64 对应 nodeID
	messageChan map[string]chan struct{}
	nextIndex   map[string]int64
	matchIndex  map[string]int64
}

func NewRaft() *Raft {
	raft := new(Raft)
	conf := NewNodeConfig()
	raft.nodeID = conf.NodeID
	peers := conf.Peers
	raft.peers = conf.Peers
	raft.messageChan = make(map[string]chan struct{}, 1)
	for _, peer := range peers {
		conn, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("err:", err)
			return nil
		}

		client := pb.NewRaftInternalClient(conn)
		raft.peersConn = append(raft.peersConn, client)
		raft.messageChan[peer.ID] = make(chan struct{}, 1)
	}

	raft.ctx = context.Background()
	raft.nodeID = tools.CreateID()
	raft.currentTerm = 0
	raft.role = Follower
	raft.votedFor = ""
	raft.logMgr = NewLogManager()
	raft.commitIndex = 0
	raft.lastApplied = 0
	raft.heartbeatDuration = time.Millisecond * 500
	raft.applyCond = sync.NewCond(&raft.mu)
	raft.electionTimer = time.NewTimer(tools.RandDuration(1000, 2000))
	// raft.heartbeatTimer = time.NewTimer(raft.heartbeatDuration)
	raft.voteCh = make(chan *pb.RequestVoteReply, len(raft.peers))
	raft.votesReceived = make(map[string]bool)
	raft.nextIndex = make(map[string]int64)
	raft.matchIndex = make(map[string]int64)
	raft.applyCh = make(chan ApplyMsg, 100)
	return raft
}

func (raft *Raft) Start() {

}

func (raft *Raft) Run() {
	for {
		select {
		case <-raft.electionTimer.C:
			raft.startElection()
			raft.electionTimer.Reset(tools.RandDuration(1000, 2000))

		case resp := <-raft.voteCh:
			raft.handleResponse(resp)
		case <-raft.heartbeatTimer.C:
			raft.sendHeartbeat()
		}
	}
}

func (raft *Raft) startElection() {
	raft.mu.Lock()
	if raft.role == Leader {
		raft.mu.Unlock()
		return
	}

	raft.role = Candidate
	raft.currentTerm++
	raft.votedFor = raft.nodeID
	// 关键：清空上一任期的选票快照
	raft.votesReceived = make(map[string]bool)

	// 关键：锁定当前任期的快照传给协程
	term := raft.currentTerm
	nodeID := raft.nodeID
	lastLogIndex, lastLogTerm := raft.logMgr.GetLastLogInfo()
	raft.mu.Unlock()

	for _, client := range raft.peersConn {
		go raft.SendRequestVoteArgs(client, nodeID, term, lastLogIndex, lastLogTerm)
	}
}

func (raft *Raft) handleResponse(resp *pb.RequestVoteReply) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	if resp.Term > raft.currentTerm {
		raft.currentTerm = resp.Term
		raft.role = Follower
		raft.votedFor = ""
		return
	}

	if raft.role != Candidate || resp.Term < raft.currentTerm {
		return
	}

	raft.votesReceived[resp.PeerId] = resp.VoteGranted

	grantedCount := 1 // 初始 1 票（自己）
	for _, granted := range raft.votesReceived {
		if granted {
			grantedCount++
		}
	}

	if grantedCount > (len(raft.peersConn)+1)/2 {
		raft.becomeLeader()
	}
}

// becomeLeader 初始化 Leader 状态并开启同步协程
func (raft *Raft) becomeLeader() {
	raft.role = Leader
	// 停止选举计时器
	if !raft.electionTimer.Stop() {
		select {
		case <-raft.electionTimer.C:
		default:
		}
	}

	lastLogIndex, _ := raft.logMgr.GetLastLogInfo()

	// 初始化所有 Peer 的进度
	for _, peer := range raft.peers {
		// 乐观探测：从自己最后一条日志的下一条开始
		raft.nextIndex[peer.ID] = lastLogIndex + 1
		// 保守确认：目前尚未确认任何匹配
		raft.matchIndex[peer.ID] = 0

		// 为每个 Peer 启动专属的复制协程
		go raft.Replicator(peer.ID)
	}

	fmt.Printf("[Node %s] Elected Leader, Term: %d\n", raft.nodeID, raft.currentTerm)
}

func (raft *Raft) sendHeartbeat() {
	for _, client := range raft.peersConn {
		go client.AppendEntries(raft.ctx, &pb.AppendEntriesArgs{})
	}
}

// Replicator 是专属 Peer 的同步协程，处理心跳与日志推送
func (raft *Raft) Replicator(peerID string) {
	// 获取该 peer 对应的 gRPC 客户端
	var targetClient pb.RaftInternalClient
	for i, p := range raft.peers {
		if p.ID == peerID {
			targetClient = raft.peersConn[i]
			break
		}
	}

	// 只要是 Leader 且节点没挂，就持续运行
	for !raft.killed() {
		raft.mu.RLock()
		if raft.role != Leader {
			raft.mu.RUnlock()
			return
		}
		raft.mu.RUnlock()

		select {
		case <-raft.messageChan[peerID]: // 收到新日志信号
		case <-time.After(raft.heartbeatDuration): // 达到心跳超时
		}

		// 执行具体的同步请求
		raft.replicateTo(peerID, targetClient)
	}
}

// replicateTo 构造并发送 AppendEntries RPC
func (raft *Raft) replicateTo(peerID string, client pb.RaftInternalClient) {
	raft.mu.RLock()
	if raft.role != Leader {
		raft.mu.RUnlock()
		return
	}

	next := raft.nextIndex[peerID]
	lastIndex, _ := raft.logMgr.GetLastLogInfo()

	// 检查是否有日志需要发送（Batch 模式）
	var entries []*pb.LogEntry
	if lastIndex >= next {
		// 从 logMgr 获取 next 之后的所有日志（工业级通常会在这里限制 maxEntries 数量）
		rawEntries := raft.logMgr.GetEntriesFrom(next)
		for _, ent := range rawEntries {
			entries = append(entries, &pb.LogEntry{
				Index:   ent.Index,
				Term:    ent.Term,
				Command: ent.Command,
			})
		}
	}

	prevIndex := next - 1
	prevTerm := raft.logMgr.GetTermAtIndex(prevIndex)

	args := &pb.AppendEntriesArgs{
		Term:         raft.currentTerm,
		LeaderId:     raft.nodeID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: raft.commitIndex,
	}
	raft.mu.RUnlock()

	// 发送 RPC（不持有锁，避免网络阻塞导致整个集群僵死）
	resp, err := client.AppendEntries(raft.ctx, args)
	if err != nil {
		return
	}

	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 1. 任期检查：如果对方任期更高，立即退位
	if resp.Term > raft.currentTerm {
		raft.currentTerm = resp.Term
		raft.role = Follower
		raft.votedFor = ""
		raft.resetElectionTimer()
		return
	}

	// 2. 状态合法性检查
	if raft.role != Leader || args.Term != raft.currentTerm {
		return
	}

	// 3. 处理回复
	if resp.Success {
		// 更新 matchIndex 和 nextIndex
		newMatch := args.PrevLogIndex + int64(len(args.Entries))
		if newMatch > raft.matchIndex[peerID] {
			raft.matchIndex[peerID] = newMatch
			raft.nextIndex[peerID] = newMatch + 1
		}
		// 每次成功同步后，尝试推进 commitIndex
		raft.advanceCommitIndex()
	} else {
		// 快速回退逻辑：利用 Follower 返回的冲突信息直接跨 Term 跳跃
		if resp.ConflictIndex > 0 {
			raft.nextIndex[peerID] = resp.ConflictIndex
		} else {
			raft.nextIndex[peerID] = max(1, raft.nextIndex[peerID]-1)
		}

		// 失败后立即重新尝试（可选，取决于对同步速度的要求）
		select {
		case raft.messageChan[peerID] <- struct{}{}:
		default:
		}
	}
}

// advanceCommitIndex 统计 matchIndex 并尝试更新 commitIndex
func (raft *Raft) advanceCommitIndex() {
	if raft.role != Leader {
		return
	}

	lastIndex, _ := raft.logMgr.GetLastLogInfo()

	// 从当前的 commitIndex 往后找
	for n := lastIndex; n > raft.commitIndex; n-- {
		// 关键点：只能提交当前任期的日志（Raft 5.4.2）
		if raft.logMgr.GetTermAtIndex(n) != raft.currentTerm {
			continue
		}

		count := 1 // 算上 Leader 自己
		for _, mIdx := range raft.matchIndex {
			if mIdx >= n {
				count++
			}
		}

		// 如果大多数节点已同步
		if count > (len(raft.peersConn)+1)/2 {
			raft.commitIndex = n
			// 唤醒 applyLoop 协程应用到状态机
			raft.applyCond.Signal()
			break
		}
	}
}

// killed 检查节点是否已停止
func (raft *Raft) killed() bool {
	// 这里可以根据你之前定义的 dead 变量进行原子检查
	return false
}

// SendRequestVoteArgs 发送投票请求
func (raft *Raft) SendRequestVoteArgs(client pb.RaftInternalClient, nodeID string, term, lastLogIndex, lastLogTerm int64) {
	resp, err := client.RequestVote(raft.ctx, &pb.RequestVoteArgs{
		Term:         term,
		CandidateId:  nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		fmt.Println("err:", err)
		return
	}

	raft.voteCh <- resp
}

func (raft *Raft) SendAppendEntriesArgs(client pb.RaftInternalClient, term, prevLogIndex, prevLogTerm int64) *pb.AppendEntriesReply {
	resp, err := client.AppendEntries(raft.ctx, &pb.AppendEntriesArgs{
		Term:         term,
		LeaderId:     raft.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: raft.commitIndex,
	})

	if err != nil {
		fmt.Println("err:", err)
		return nil
	}

	return resp
}

func (raft *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteArgs, opts ...grpc.CallOption) (*pb.RequestVoteReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	clear(raft.votesReceived)

	resp := pb.RequestVoteReply{
		Term:        raft.currentTerm,
		VoteGranted: false,
		PeerId:      raft.nodeID,
	}

	if req.Term < raft.currentTerm {
		return &resp, nil
	}

	if req.Term > raft.currentTerm {
		raft.currentTerm = req.Term
		raft.role = Follower
		raft.votedFor = ""
		// 这里可能需要持久化
	}

	if raft.votedFor == "" || raft.votedFor == req.CandidateId { // 这里的条件是防止网络波动 nodeA向nodeB发送请求 A做出了响应 但是响应过程中丢失了 nodeB可能会重试
		lastLogIndex, lastLogTerm := raft.logMgr.GetLastLogInfo()

		isVoted := false
		if req.LastLogTerm > lastLogTerm {
			isVoted = true
		} else if lastLogTerm == req.LastLogTerm && req.LastLogIndex >= lastLogIndex {
			isVoted = true
		}

		if isVoted {
			raft.role = Follower
			raft.votedFor = req.CandidateId
			raft.resetElectionTimer()
			resp.VoteGranted = true
		}

	}

	resp.Term = raft.currentTerm
	return &resp, nil
}

func (raft *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs, opts ...grpc.CallOption) (*pb.AppendEntriesReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	resp := &pb.AppendEntriesReply{
		Term:    raft.currentTerm,
		Success: false,
	}

	// 任期检查：对方任期比我小，直接拒绝
	if req.Term < raft.currentTerm {
		return resp, nil
	}

	// 发现更高任期或合法的现任 Leader，重置状态
	// 只要 req.Term >= rf.currentTerm，就承认对方是有效 Leader
	if req.Term > raft.currentTerm {
		raft.currentTerm = req.Term
		raft.votedFor = ""
		// 持久化更新后的 Term
	}

	raft.role = Follower
	raft.resetElectionTimer() // 只有这里重置了，Follower 才不会造反

	// 日志一致性检查 (PrevLogIndex & PrevLogTerm)
	if !raft.logMgr.MatchLog(req.PrevLogIndex, req.PrevLogTerm) {
		// 快速回溯逻辑：告诉 Leader 我这里哪里不匹配，让他一次跳过一个 Term
		resp.ConflictIndex, resp.ConflictTerm = raft.logMgr.GetConflictInfo(req.PrevLogIndex)
		resp.Term = raft.currentTerm
		return resp, nil
	}

	// 写入日志并处理冲突 (Truncate & Append)
	for i, entry := range req.Entries {
		// 如果遇到 Index 相同但 Term 不同的，说明冲突了，删除后面所有并覆盖
		if !raft.logMgr.IsEntryMatch(entry.Index, entry.Term) {
			raft.logMgr.TruncateFrom(entry.Index)
			raft.logMgr.Append(req.Entries[i:])
			break
		}
	}

	// 5. 更新本地 commitIndex
	if req.LeaderCommit > raft.commitIndex {
		// commitIndex 不能超过本地最新日志的索引
		lastIdx, _ := raft.logMgr.GetLastLogInfo()
		raft.commitIndex = min(req.LeaderCommit, lastIdx)

		// 唤醒 apply 协程，把日志写入 s.store
		raft.applyCond.Signal()
	}

	resp.Success = true
	resp.Term = raft.currentTerm
	return resp, nil
}

func (raft *Raft) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs, opts ...grpc.CallOption) (*pb.InstallSnapshotReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	resp := &pb.InstallSnapshotReply{Term: raft.currentTerm}

	// 1. 任期检查
	if req.Term < raft.currentTerm {
		return resp, nil
	}

	// 2. 状态机转换
	if req.Term > raft.currentTerm {
		raft.currentTerm = req.Term
		raft.votedFor = ""
		raft.role = Follower
		// raft.persist() // 之后统一在 SaveStateAndSnapshot 处理
	}

	// 3. 幂等检查：如果本地快照已经比请求的新，直接跳过
	if req.LastIncludedIndex <= raft.logMgr.lastIncludedIndex {
		return resp, nil
	}

	// 4. 日志对齐
	if raft.logMgr.IsEntryMatch(req.LastIncludedIndex, req.LastIncludedTerm) {
		// 如果快照点匹配，保留后面的日志（减少同步开销）
		raft.logMgr.TruncateBefore(req.LastIncludedIndex, req.LastIncludedTerm)
	} else {
		// 如果不匹配，彻底清空，从快照开始新生活
		raft.logMgr.ResetWithSnapshot(req.LastIncludedIndex, req.LastIncludedTerm)
	}

	// 5. 核心持久化：将 Raft 状态和快照二进制数据存入 Persister
	//raft.persister.SaveStateAndSnapshot(raft.serializeState(), req.Data)

	// 6. 推进进度
	if req.LastIncludedIndex > raft.commitIndex {
		raft.commitIndex = req.LastIncludedIndex
	}
	if req.LastIncludedIndex > raft.lastApplied {
		raft.lastApplied = req.LastIncludedIndex
	}

	// 7. 异步通知上层 KV Server 应用快照
	go func(data []byte, term, index int64) {
		raft.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotTerm:  int(term),
			SnapshotIndex: int(index),
		}
	}(req.Data, req.LastIncludedTerm, req.LastIncludedIndex)

	return resp, nil
}

func (raft *Raft) resetElectionTimer() {
	if !raft.electionTimer.Stop() {
		select {
		case <-raft.electionTimer.C: // 如果已经到期了，把它读出来丢掉
		default:
		}
	}
	raft.electionTimer.Reset(tools.RandDuration(1000, 2000))
}

func (raft *Raft) applyLoop() {
	for {
		raft.mu.Lock()

		// 如果没有新日志需要 apply，就一直在这等（释放锁并阻塞）
		for raft.lastApplied >= raft.commitIndex {
			raft.applyCond.Wait() // 会自动释放 rf.mu，并阻塞在这里
			// 被唤醒后，Wait() 会重新抢到 rf.mu 锁
		}

		// 发现 commitIndex 推进了，准备批量取出日志
		if raft.lastApplied < raft.logMgr.lastIncludedIndex {
			// 说明中间这段日志已经通过快照直接更新到状态机了
			// 我们直接跳过这部分，更新进度条
			raft.lastApplied = raft.logMgr.lastIncludedIndex
			raft.mu.Unlock()
			continue
		}
		commitIndex := raft.commitIndex
		lastApplied := raft.lastApplied

		// 找出需要发送给状态机的日志切片 并且备份 释放锁
		entries := make([]LogEntry, 0)
		for i := lastApplied + 1; i <= commitIndex; i++ {
			// 这里用到你之前的 getPhysicalIndex 逻辑
			pIdx := raft.logMgr.getPhysicalIndex(i)
			entries = append(entries, raft.logMgr.entries[pIdx])
		}

		raft.mu.Unlock() // 发送 channel 前先解锁，避免阻塞

		// 把日志应用到状态机（kv server）
		for _, entry := range entries {
			raft.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int(entry.Index),
			}
		}

		// 更新 lastApplied
		raft.mu.Lock()
		raft.lastApplied = max(raft.lastApplied, commitIndex)
		raft.mu.Unlock()
	}
}
