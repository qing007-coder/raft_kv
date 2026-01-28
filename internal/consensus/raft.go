package consensus

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	pb "raft_kv/internal/consensus/proto"
	"raft_kv/internal/tools"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Raft 实现了 Raft 共识算法的核心逻辑
type Raft struct {
	mu         sync.RWMutex                     // 保护并发访问的锁
	ctx        context.Context                  // 上下文，用于控制协程生命周期
	peersConn  map[string]pb.RaftInternalClient // 与其他节点的 gRPC 连接
	peers      []Node                           // 集群中其他节点的信息
	nodeID     string                           // 当前节点的 ID
	dead       int32                            // 节点是否已关闭的标志
	grpcServer *grpc.Server                     // gRPC 服务器实例
	addr       string                           // 当前节点的 gRPC 地址
	pb.UnimplementedRaftInternalServer

	// 持久化状态 - 这些字段会被保存到磁盘
	persister   *Persister  // 负责状态持久化
	currentTerm int64       // 当前任期号
	votedFor    string      // 当前任期内投票给的节点 ID
	logMgr      *LogManager // 日志管理器

	// 内存状态 这些字段只存在于内存中
	commitIndex int64    // 已提交的最高日志索引
	lastApplied int64    // 已应用到状态机的最高日志索引
	role        NodeRole // 当前节点的角色（Follower、Candidate、Leader）

	// 状态通知
	applyCh   chan *ApplyMsg // 应用消息的通道
	applyCond *sync.Cond     // 用于通知 applyLoop 有新的提交日志

	// 计时器相关
	electionTimer     *time.Timer               // 选举超时计时器
	heartbeatTimer    *time.Ticker              // 心跳计时器
	heartbeatDuration time.Duration             // 心跳间隔
	voteCh            chan *pb.RequestVoteReply // 接收投票回复的通道
	votesReceived     map[string]bool           // 记录已收到的投票

	// Leader 独有状态
	messageChan map[string]chan struct{} // 向每个节点发送复制消息的通道
	nextIndex   map[string]int64         // 每个节点下一个需要复制的日志索引
	matchIndex  map[string]int64         // 每个节点已复制的最高日志索引
	proposalMap map[int64]chan *ApplyMsg // 日志索引 -> 结果 channel，用于等待命令提交
}

// NewRaft 创建一个新的 Raft 节点
// applyCh: 应用日志到状态机的通道
// configPath: 配置文件路径
func NewRaft(applyCh chan *ApplyMsg, configPath string) *Raft {
	raft := new(Raft)
	conf := NewNodeConfig(configPath) // 获取节点配置
	raft.peers = conf.Peers
	raft.peersConn = make(map[string]pb.RaftInternalClient)
	raft.nodeID = conf.NodeID
	raft.addr = conf.Addr
	fmt.Println(conf.DataDir)
	raft.persister = NewPersister(conf.DataDir)
	raft.applyCh = applyCh
	raft.logMgr = NewLogManager()

	// 初始化与其他节点的连接和消息通道
	raft.messageChan = make(map[string]chan struct{}, 1)
	for _, peer := range raft.peers {
		// 建立 gRPC 连接
		conn, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("Connect to peer %s failed: %v\n", peer.ID, err)
			continue
		}
		client := pb.NewRaftInternalClient(conn)

		raft.peersConn[peer.ID] = client
		raft.messageChan[peer.ID] = make(chan struct{}, 1)
	}

	// 初始化基本状态
	raft.ctx = context.Background()
	raft.role = Follower
	raft.currentTerm = 0
	raft.votedFor = ""
	raft.commitIndex = 0
	raft.lastApplied = 0
	raft.heartbeatDuration = time.Millisecond * 150 // 心跳间隔设为 150ms

	// 从持久化存储中恢复状态
	raft.decodeState(raft.persister.ReadRaftState())

	// 同步提交和应用索引与日志管理器的快照信息
	raft.commitIndex = raft.logMgr.lastIncludedIndex
	raft.lastApplied = raft.logMgr.lastIncludedIndex

	// 初始化同步原语和计时器
	raft.applyCond = sync.NewCond(&raft.mu)
	raft.electionTimer = time.NewTimer(tools.RandDuration(1000, 2000)) // 选举超时设为 1-2s
	raft.voteCh = make(chan *pb.RequestVoteReply, len(raft.peers))
	raft.votesReceived = make(map[string]bool)
	raft.nextIndex = make(map[string]int64)
	raft.matchIndex = make(map[string]int64)
	raft.proposalMap = make(map[int64]chan *ApplyMsg)

	// 启动 gRPC 服务器
	raft.grpcServer = grpc.NewServer()
	pb.RegisterRaftInternalServer(raft.grpcServer, raft)

	// 在新协程中启动 gRPC 服务器
	go func() {
		listener, err := net.Listen("tcp", raft.addr)
		if err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
		log.Printf("gRPC server started on %s", raft.addr)
		if err := raft.grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	return raft
}

// persist 将 Raft 的核心状态序列化并保存到磁盘
func (raft *Raft) persist() {
	data := raft.encodeState()
	raft.persister.SaveStateAndSnapshot(data, raft.persister.ReadSnapshot())
}

// Start 启动 Raft 节点的核心协程
// 包括运行状态机和应用日志到状态机的协程
func (raft *Raft) Start() {
	go raft.Run()
	go raft.applyLoop()
}

// Run 运行 Raft 节点的主循环
// 处理选举超时、投票回复和心跳事件
func (raft *Raft) Run() {
	// 初始化心跳计时器
	raft.heartbeatTimer = time.NewTicker(raft.heartbeatDuration)
	defer raft.heartbeatTimer.Stop()

	for {
		select {
		case <-raft.electionTimer.C:
			// 选举超时，开始新一轮选举
			raft.startElection()

		case resp := <-raft.voteCh:
			// 收到投票回复，处理投票结果
			raft.handleResponse(resp)

		case <-raft.heartbeatTimer.C:
			// 心跳计时器触发，发送心跳
			raft.sendHeartbeat()

		case <-raft.ctx.Done():
			// 上下文取消，退出主循环
			return
		}
	}
}

// Propose 向 Raft 集群提交一个命令
// 只有 Leader 节点可以处理提议
// 返回值：日志索引、当前任期、是否为 Leader
func (raft *Raft) Propose(command []byte) (index int64, term int64, isLeader bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 只有 Leader 可以处理提议
	if raft.role != Leader {
		return -1, -1, false
	}

	// 计算新日志的索引
	lastIndex, _ := raft.logMgr.GetLastLogInfo()
	newIndex := lastIndex + 1
	currentTerm := raft.currentTerm

	// 创建新的日志条目
	newEntry := &pb.LogEntry{
		Index: newIndex,
		Term:  currentTerm,
		Data:  command,
	}

	// 将新日志追加到日志管理器
	raft.logMgr.Append([]*pb.LogEntry{newEntry})
	// 写入新日志后立即持久化，确保崩溃后可以恢复
	raft.persist()

	// 通知所有 Follower 节点复制新日志
	for peerID := range raft.messageChan {
		select {
		case raft.messageChan[peerID] <- struct{}{}:
		default:
			// 如果通道已满，跳过通知，等待下一次心跳
		}
	}

	return newIndex, currentTerm, true
}

// ProposeWithCallback 向 Raft 集群提交一个命令，并在命令提交后通过 channel 返回结果
// 只有 Leader 节点可以处理提议
// 返回值：日志索引、当前任期、是否为 Leader
func (raft *Raft) ProposeWithCallback(command []byte, resultCh chan *ApplyMsg) (index int64, term int64, isLeader bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 只有 Leader 可以处理提议
	if raft.role != Leader {
		return -1, -1, false
	}

	// 计算新日志的索引
	lastIndex, _ := raft.logMgr.GetLastLogInfo()
	newIndex := lastIndex + 1
	currentTerm := raft.currentTerm

	// 创建新的日志条目
	newEntry := &pb.LogEntry{
		Index: newIndex,
		Term:  currentTerm,
		Data:  command,
	}

	// 将新日志追加到日志管理器
	raft.logMgr.Append([]*pb.LogEntry{newEntry})
	// 写入新日志后立即持久化，确保崩溃后可以恢复
	raft.persist()

	// 记录请求上下文，将日志索引与结果 channel 关联
	raft.proposalMap[newIndex] = resultCh

	// 通知所有 Follower 节点复制新日志
	for peerID := range raft.messageChan {
		select {
		case raft.messageChan[peerID] <- struct{}{}:
		default:
			// 如果通道已满，跳过通知，等待下一次心跳
		}
	}

	return newIndex, currentTerm, true
}

// startElection 开始新一轮选举
// 当选举超时或接收到更高任期的请求时调用
func (raft *Raft) startElection() {
	// 重置选举计时器，避免重复触发选举
	raft.resetElectionTimer()
	raft.mu.Lock()
	// 如果当前已经是 Leader，不需要选举
	if raft.role == Leader {
		raft.mu.Unlock()
		return
	}

	// 转换为 Candidate 状态，开始选举
	raft.role = Candidate
	// 增加任期号
	raft.currentTerm++
	// 投票给自己
	raft.votedFor = raft.nodeID
	// 重置投票记录，确保只记录当前任期的投票
	raft.votesReceived = make(map[string]bool)

	// 记录选举开始
	term := raft.currentTerm
	nodeID := raft.nodeID
	log.Printf("Node %s starting election for term %d", nodeID, term)

	// 持久化状态，确保崩溃后可以恢复
	raft.persist()

	lastLogIndex, lastLogTerm := raft.logMgr.GetLastLogInfo()
	raft.mu.Unlock()

	// 向所有其他节点发送投票请求
	log.Printf("Node %s sending vote requests to all peers for term %d", nodeID, term)
	for _, client := range raft.peersConn {
		go raft.SendRequestVoteArgs(client, nodeID, term, lastLogIndex, lastLogTerm)
	}
}

// handleResponse 处理投票请求的回复
// 统计投票结果，决定是否成为 Leader
func (raft *Raft) handleResponse(resp *pb.RequestVoteReply) {
	raft.mu.Lock()

	// 如果收到更高任期的回复，更新自己的任期并转为 Follower
	if resp.Term > raft.currentTerm {
		log.Printf("Node %s received higher term %d from %s, converting to Follower", raft.nodeID, resp.Term, resp.PeerId)
		raft.currentTerm = resp.Term
		raft.role = Follower
		raft.votedFor = ""
		// 持久化状态变化
		raft.persist()
		// 重置选举计时器
		raft.resetElectionTimer()
		raft.mu.Unlock()
		return
	}

	// 如果当前不是 Candidate，或者回复的任期小于当前任期，忽略该回复
	if raft.role != Candidate || resp.Term < raft.currentTerm {
		raft.mu.Unlock()
		return
	}

	// 记录投票结果
	raft.votesReceived[resp.PeerId] = resp.VoteGranted
	log.Printf("Node %s received vote from %s: %t for term %d", raft.nodeID, resp.PeerId, resp.VoteGranted, resp.Term)

	// 统计获得的赞成票数量
	grantedCount := 1 // 自己的一票
	for _, granted := range raft.votesReceived {
		if granted {
			grantedCount++
		}
	}

	// 计算集群总节点数（包括自己）
	totalNodes := len(raft.peersConn) + 1
	log.Printf("Node %s has received %d votes out of %d needed for term %d", raft.nodeID, grantedCount, (totalNodes/2)+1, raft.currentTerm)

	raft.mu.Unlock()
	// 如果获得超过半数的赞成票，成为 Leader
	if grantedCount > totalNodes/2 {
		raft.becomeLeader()
	}
}

// becomeLeader 转换为 Leader 状态
// 当获得超过半数的投票时调用
func (raft *Raft) becomeLeader() {
	raft.mu.Lock()

	// 转换为 Leader 状态
	raft.role = Leader
	// 停止选举计时器，因为 Leader 不需要选举
	raft.electionTimer.Stop()

	// 获取当前最后一条日志的索引
	lastLogIndex, _ := raft.logMgr.GetLastLogInfo()

	raft.mu.Unlock()
	// 初始化每个 Follower 的 nextIndex 和 matchIndex
	for _, peer := range raft.peers {
		raft.mu.Lock()
		// nextIndex 初始化为最后一条日志的下一个位置
		raft.nextIndex[peer.ID] = lastLogIndex + 1
		// matchIndex 初始化为 0
		raft.matchIndex[peer.ID] = 0
		raft.mu.Unlock()

		// 为每个 Follower 启动一个复制协程
		go raft.Replicator(peer.ID)
	}

	// 记录成为 Leader
	log.Printf("Node %s became Leader for term %d", raft.nodeID, raft.currentTerm)
	// 发送初始心跳，通知其他节点自己成为了 Leader
	raft.sendHeartbeat()
}

// sendHeartbeat 发送心跳消息给所有 Follower
// 保持 Leader 地位，防止 Follower 超时选举
func (raft *Raft) sendHeartbeat() {
	raft.mu.RLock()
	defer raft.mu.RUnlock()

	// 只有 Leader 才需要发送心跳
	if raft.role != Leader {
		return
	}

	// 记录发送心跳
	log.Printf("Leader %s sending heartbeat for term %d", raft.nodeID, raft.currentTerm)

	// 通知所有 Follower 节点发送心跳（空日志条目）
	for _, peer := range raft.peers {
		select {
		case raft.messageChan[peer.ID] <- struct{}{}:
		default:
			// 如果通道已满，跳过通知，等待下一次心跳
		}
	}
}

// Replicator 是 Leader 节点上的复制协程
// 负责将日志复制到指定的 Follower 节点
func (raft *Raft) Replicator(peerID string) {
	// 找到目标节点的 gRPC 客户端
	targetClient := raft.peersConn[peerID]

	// 只要节点未关闭且仍是 Leader，就持续复制日志
	for !raft.killed() {
		raft.mu.RLock()
		// 如果不再是 Leader，退出复制协程
		if raft.role != Leader {
			raft.mu.RUnlock()
			return
		}
		raft.mu.RUnlock()

		// 等待复制通知或心跳超时
		select {
		case <-raft.messageChan[peerID]:
			// 收到复制通知，立即执行复制
		case <-time.After(raft.heartbeatDuration):
			// 心跳超时，发送心跳（空日志）
		}

		// 执行日志复制
		raft.replicateTo(peerID, targetClient)
	}
}

// replicateTo 将日志复制到指定的 Follower 节点
// 是日志复制的核心逻辑
func (raft *Raft) replicateTo(peerID string, client pb.RaftInternalClient) {
	raft.mu.RLock()
	// 如果不再是 Leader，停止复制
	if raft.role != Leader {
		raft.mu.RUnlock()
		return
	}

	// 获取下一个需要复制的日志索引
	next := raft.nextIndex[peerID]
	// 获取当前最后一条日志的索引
	lastIndex, _ := raft.logMgr.GetLastLogInfo()

	// 如果 nextIndex 已经被包含在快照中，需要发送快照
	if next <= raft.logMgr.lastIncludedIndex {
		// 准备快照数据
		lastIncludedIndex := raft.logMgr.lastIncludedIndex
		lastIncludedTerm := raft.logMgr.lastIncludedTerm
		snapshotData := raft.persister.ReadSnapshot()
		raft.mu.RUnlock()

		// 构造 InstallSnapshot 请求
		snapshotArgs := &pb.InstallSnapshotArgs{
			Term:              raft.currentTerm,
			LeaderId:          raft.nodeID,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Data:              snapshotData,
		}

		// 记录发送快照请求
		log.Printf("Leader %s sending InstallSnapshot to %s: lastIncludedIndex=%d, lastIncludedTerm=%d",
			raft.nodeID, peerID, lastIncludedIndex, lastIncludedTerm)

		// 发送 InstallSnapshot RPC
		snapshotResp, err := client.InstallSnapshot(raft.ctx, snapshotArgs)
		if err != nil {
			// 网络错误，暂时忽略，等待下一次重试
			log.Printf("Leader %s failed to send InstallSnapshot to %s: %v", raft.nodeID, peerID, err)
			return
		}

		raft.mu.Lock()
		defer raft.mu.Unlock()

		// 如果收到更高任期的回复，更新自己的任期并转为 Follower
		if snapshotResp.Term > raft.currentTerm {
			log.Printf("Leader %s received higher term %d from %s, converting to Follower", raft.nodeID, snapshotResp.Term, peerID)
			raft.currentTerm = snapshotResp.Term
			raft.role = Follower
			raft.votedFor = ""
			// 持久化状态变化
			raft.persist()
			// 重置选举计时器
			raft.resetElectionTimer()
			return
		}

		// 更新 nextIndex 和 matchIndex
		raft.nextIndex[peerID] = lastIncludedIndex + 1
		raft.matchIndex[peerID] = lastIncludedIndex
		log.Printf("Leader %s updated nextIndex[%s] to %d, matchIndex[%s] to %d after snapshot",
			raft.nodeID, peerID, raft.nextIndex[peerID], peerID, raft.matchIndex[peerID])

		// 尝试推进 commitIndex
		raft.advanceCommitIndex()
		return
	}

	// 准备需要复制的日志条目
	var entries []*pb.LogEntry
	if lastIndex >= next {
		// 获取从 next 开始的所有日志条目
		entries = raft.logMgr.GetEntriesFrom(next)
	}

	// 计算前一个日志条目的索引和任期
	prevIndex := next - 1
	prevTerm := raft.logMgr.GetTermAtIndex(prevIndex)

	// 构造 AppendEntries 请求
	args := &pb.AppendEntriesArgs{
		Term:         raft.currentTerm, // 当前任期
		LeaderId:     raft.nodeID,      // Leader 的 ID
		PrevLogIndex: prevIndex,        // 前一个日志条目的索引
		PrevLogTerm:  prevTerm,         // 前一个日志条目的任期
		Entries:      entries,          // 需要复制的日志条目
		LeaderCommit: raft.commitIndex, // Leader 的提交索引
	}
	raft.mu.RUnlock()

	// 记录发送 AppendEntries 请求
	log.Printf("Leader %s sending AppendEntries to %s: prevIndex=%d, prevTerm=%d, entriesCount=%d", raft.nodeID, peerID, prevIndex, prevTerm, len(entries))

	// 发送 AppendEntries RPC
	resp, err := client.AppendEntries(raft.ctx, args)
	if err != nil {
		// 网络错误，暂时忽略，等待下一次重试
		log.Printf("Leader %s failed to send AppendEntries to %s: %v", raft.nodeID, peerID, err)
		return
	}

	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 如果收到更高任期的回复，更新自己的任期并转为 Follower
	if resp.Term > raft.currentTerm {
		log.Printf("Leader %s received higher term %d from %s, converting to Follower", raft.nodeID, resp.Term, peerID)
		raft.currentTerm = resp.Term
		raft.role = Follower
		raft.votedFor = ""
		// 持久化状态变化
		raft.persist()
		// 重置选举计时器
		raft.resetElectionTimer()
		return
	}

	// 如果当前不再是 Leader，或者请求的任期与当前任期不符，忽略回复
	if raft.role != Leader || args.Term != raft.currentTerm {
		return
	}

	// 处理复制成功的情况
	if resp.Success {
		// 计算新的匹配索引
		newMatch := args.PrevLogIndex + int64(len(args.Entries))
		// 更新匹配索引和下一个索引
		if newMatch > raft.matchIndex[peerID] {
			log.Printf("Leader %s successfully replicated logs to %s, new matchIndex=%d", raft.nodeID, peerID, newMatch)
			raft.matchIndex[peerID] = newMatch
			raft.nextIndex[peerID] = newMatch + 1
		}
		// 尝试推进提交索引
		raft.advanceCommitIndex()
	} else {
		// 处理复制失败的情况
		log.Printf("Leader %s failed to replicate logs to %s, conflictIndex=%d", raft.nodeID, peerID, resp.ConflictIndex)
		if resp.ConflictIndex > 0 {
			// 根据冲突索引调整下一个索引
			// 确保下一个索引不会小于快照的最后索引
			raft.nextIndex[peerID] = max(raft.logMgr.lastIncludedIndex+1, resp.ConflictIndex)
		} else {
			// 线性查找，逐个回退
			// 确保下一个索引不会小于快照的最后索引
			raft.nextIndex[peerID] = max(raft.logMgr.lastIncludedIndex+1, raft.nextIndex[peerID]-1)
		}
		// 重新尝试复制
		select {
		case raft.messageChan[peerID] <- struct{}{}:
		default:
		}
	}
}

// advanceCommitIndex 尝试推进提交索引
// 当有新的日志被复制到多数节点时调用
func (raft *Raft) advanceCommitIndex() {
	// 获取当前最后一条日志的索引
	lastIndex, _ := raft.logMgr.GetLastLogInfo()
	// 计算集群总节点数（包括自己）
	totalNodes := len(raft.peersConn) + 1
	// 从最后一条日志开始，向前查找可以提交的日志
	for n := lastIndex; n > raft.commitIndex; n-- {
		// 只考虑当前任期的日志
		if raft.logMgr.GetTermAtIndex(n) != raft.currentTerm {
			continue
		}

		// 统计已经复制了该日志的节点数量
		count := 1 // 自己的一票
		for _, mIdx := range raft.matchIndex {
			if mIdx >= n {
				count++
			}
		}

		// 如果超过半数的节点已经复制了该日志，提交它
		if count > totalNodes/2 {
			oldCommitIndex := raft.commitIndex
			raft.commitIndex = n
			// 通知 applyLoop 有新的日志需要应用
			raft.applyCond.Signal()
			// 记录提交索引的推进
			log.Printf("Leader %s advanced commitIndex from %d to %d for term %d", raft.nodeID, oldCommitIndex, raft.commitIndex, raft.currentTerm)
			break
		}
	}
}

// SendRequestVoteArgs 发送投票请求并处理回复
// 是选举过程中的关键函数
func (raft *Raft) SendRequestVoteArgs(client pb.RaftInternalClient, nodeID string, term, lastLogIndex, lastLogTerm int64) {
	// 构造投票请求参数
	args := &pb.RequestVoteArgs{
		Term:         term,         // 候选者的任期
		CandidateId:  nodeID,       // 候选者的 ID
		LastLogIndex: lastLogIndex, // 候选者最后一条日志的索引
		LastLogTerm:  lastLogTerm,  // 候选者最后一条日志的任期
	}

	// 记录发送投票请求
	log.Printf("Candidate %s sending vote request for term %d, lastLogIndex=%d, lastLogTerm=%d", nodeID, term, lastLogIndex, lastLogTerm)

	// 设置 RPC 超时，防止某个节点宕机导致协程永久阻塞
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	// 发送投票请求 RPC
	resp, err := client.RequestVote(ctx, args)
	if err != nil {
		// 网络错误，暂时忽略，选举过程中部分节点失败是正常的
		log.Printf("Candidate %s failed to send vote request: %v", nodeID, err)
		return
	}

	// 记录收到投票回复
	log.Printf("Candidate %s received vote reply: granted=%t, term=%d", nodeID, resp.VoteGranted, resp.Term)

	// 将投票结果推送到处理管道
	select {
	case <-raft.ctx.Done():
		// 上下文取消，退出
		return
	case raft.voteCh <- resp:
		// 成功将结果推入管道
	default:
		// 如果管道满了，说明处理速度跟不上，但在 Raft 选举中这通常意味着结果已经足够了
	}
}

// RequestVote 处理来自其他节点的投票请求
// 实现了 Raft 选举中的投票逻辑
func (raft *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 记录收到投票请求
	log.Printf("Node %s received vote request from %s for term %d, lastLogIndex=%d, lastLogTerm=%d", raft.nodeID, req.CandidateId, req.Term, req.LastLogIndex, req.LastLogTerm)

	// 初始化投票回复
	resp := &pb.RequestVoteReply{
		Term:        raft.currentTerm,
		VoteGranted: false,
		PeerId:      raft.nodeID,
	}

	// 如果请求的任期小于当前任期，拒绝投票
	if req.Term < raft.currentTerm {
		log.Printf("Node %s rejecting vote for term %d (current term is %d)", raft.nodeID, req.Term, raft.currentTerm)
		return resp, nil
	}

	// 如果请求的任期大于当前任期，更新自己的任期并转为 Follower
	if req.Term > raft.currentTerm {
		log.Printf("Node %s updating term from %d to %d and becoming Follower", raft.nodeID, raft.currentTerm, req.Term)
		raft.currentTerm = req.Term
		raft.role = Follower
		raft.votedFor = ""
		// 持久化状态变化
		raft.persist()
	}

	// 如果还没有投票，或者已经投票给了该候选者
	if raft.votedFor == "" || raft.votedFor == req.CandidateId {
		// 获取自己最后一条日志的信息
		lastLogIndex, lastLogTerm := raft.logMgr.GetLastLogInfo()

		// 检查候选者的日志是否比自己的更新
		upToDate := false
		if req.LastLogTerm > lastLogTerm {
			// 候选者的最后一条日志的任期更大，更新
			upToDate = true
		} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex {
			// 任期相同，但候选者的日志更长，更新
			upToDate = true
		}

		// 如果候选者的日志更新，授予投票
		if upToDate {
			log.Printf("Node %s granting vote to %s for term %d", raft.nodeID, req.CandidateId, req.Term)
			raft.role = Follower
			raft.votedFor = req.CandidateId
			// 持久化投票记录
			raft.persist()
			// 重置选举计时器
			raft.resetElectionTimer()
			resp.VoteGranted = true
		} else {
			log.Printf("Node %s rejecting vote to %s: log not up to date", raft.nodeID, req.CandidateId)
		}
	} else {
		log.Printf("Node %s rejecting vote to %s: already voted for %s", raft.nodeID, req.CandidateId, raft.votedFor)
	}

	// 更新回复中的任期
	resp.Term = raft.currentTerm
	log.Printf("Node %s sending vote reply to %s: granted=%t, term=%d", raft.nodeID, req.CandidateId, resp.VoteGranted, resp.Term)
	return resp, nil
}

// InstallSnapshot 处理来自 Leader 的快照安装请求
// 当 Follower 落后太多时，Leader 会发送快照而不是日志条目
func (raft *Raft) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 记录收到快照请求
	log.Printf("Node %s received InstallSnapshot from %s: term=%d, lastIncludedIndex=%d, lastIncludedTerm=%d",
		raft.nodeID, req.LeaderId, req.Term, req.LastIncludedIndex, req.LastIncludedTerm)

	// 初始化回复
	resp := &pb.InstallSnapshotReply{
		Term: raft.currentTerm,
	}

	// 如果请求的任期小于当前任期，拒绝请求
	if req.Term < raft.currentTerm {
		log.Printf("Node %s rejecting InstallSnapshot: req term %d < current term %d", raft.nodeID, req.Term, raft.currentTerm)
		return resp, nil
	}

	// 如果请求的任期大于当前任期，更新自己的任期
	if req.Term > raft.currentTerm {
		log.Printf("Node %s updating term from %d to %d and becoming Follower", raft.nodeID, raft.currentTerm, req.Term)
		raft.currentTerm = req.Term
		raft.votedFor = ""
		// 持久化状态变化
		raft.persist()
	}

	// 转为 Follower 状态，并重置选举计时器
	raft.role = Follower
	raft.resetElectionTimer()

	// 检查快照是否比当前快照更新
	if req.LastIncludedIndex <= raft.logMgr.lastIncludedIndex {
		// 快照不新，无需处理
		log.Printf("Node %s rejecting InstallSnapshot: snapshot not newer", raft.nodeID)
		return resp, nil
	}

	// 应用快照到状态机
	snapshotMsg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: int(req.LastIncludedIndex),
		SnapshotTerm:  int(req.LastIncludedTerm),
	}

	// 发送快照到 applyCh
	select {
	case raft.applyCh <- snapshotMsg:
		// 快照已发送到状态机
		log.Printf("Node %s sent snapshot to applyCh", raft.nodeID)
	default:
		// 如果通道满了，记录错误但继续处理
		log.Printf("Node %s failed to send snapshot to applyCh: channel full", raft.nodeID)
	}

	// 更新日志管理器的快照元数据
	raft.logMgr.ResetWithSnapshot(req.LastIncludedIndex, req.LastIncludedTerm)

	// 更新 commitIndex 和 lastApplied
	raft.commitIndex = req.LastIncludedIndex
	raft.lastApplied = req.LastIncludedIndex

	// 持久化状态
	raft.persist()
	log.Printf("Node %s applied snapshot up to index %d, term %d", raft.nodeID, req.LastIncludedIndex, req.LastIncludedTerm)

	// 更新回复中的任期
	resp.Term = raft.currentTerm
	return resp, nil
}

// AppendEntries 处理来自 Leader 的日志复制请求
// 也用于发送心跳，维持 Leader 地位
func (raft *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	// 记录收到 AppendEntries 请求
	log.Printf("Node %s received AppendEntries from %s: term=%d, prevLogIndex=%d, prevLogTerm=%d, entriesCount=%d, leaderCommit=%d",
		raft.nodeID, req.LeaderId, req.Term, req.PrevLogIndex, req.PrevLogTerm, len(req.Entries), req.LeaderCommit)

	// 初始化回复
	resp := &pb.AppendEntriesReply{
		Term:    raft.currentTerm,
		Success: false,
	}

	// 如果请求的任期小于当前任期，拒绝请求
	if req.Term < raft.currentTerm {
		log.Printf("Node %s rejecting AppendEntries: req term %d < current term %d", raft.nodeID, req.Term, raft.currentTerm)
		return resp, nil
	}

	// 如果请求的任期大于当前任期，更新自己的任期
	if req.Term > raft.currentTerm {
		log.Printf("Node %s updating term from %d to %d and becoming Follower", raft.nodeID, raft.currentTerm, req.Term)
		raft.currentTerm = req.Term
		raft.votedFor = ""
		// 持久化状态变化
		raft.persist()
	}

	// 转为 Follower 状态，并重置选举计时器
	raft.role = Follower
	raft.resetElectionTimer()

	// 日志对账：检查前一个日志条目是否匹配
	if !raft.logMgr.MatchLog(req.PrevLogIndex, req.PrevLogTerm) {
		// 日志不匹配，返回冲突信息
		resp.ConflictIndex, resp.ConflictTerm = raft.logMgr.GetConflictInfo(req.PrevLogIndex)
		resp.Term = raft.currentTerm
		log.Printf("Node %s rejecting AppendEntries: log mismatch at index %d, term %d", raft.nodeID, req.PrevLogIndex, req.PrevLogTerm)
		return resp, nil
	}

	// 写入日志：只添加新的日志条目
	logChanged := false
	for i, entry := range req.Entries {
		// 如果日志条目不匹配，截断并添加新日志
		if !raft.logMgr.IsEntryMatch(entry.Index, entry.Term) {
			raft.logMgr.TruncateFrom(entry.Index)
			raft.logMgr.Append(req.Entries[i:])
			logChanged = true
			log.Printf("Node %s appending log entries from index %d", raft.nodeID, entry.Index)
			break
		}
	}

	// 如果日志发生变化，持久化
	if logChanged {
		raft.persist()
		log.Printf("Node %s persisted log changes", raft.nodeID)
	}

	// 更新提交索引
	if req.LeaderCommit > raft.commitIndex {
		lastIdx, _ := raft.logMgr.GetLastLogInfo()
		oldCommitIndex := raft.commitIndex
		// 提交索引不能超过最后一条日志的索引
		raft.commitIndex = min(req.LeaderCommit, lastIdx)
		// 通知 applyLoop 有新的日志需要应用
		raft.applyCond.Signal()
		log.Printf("Node %s advanced commitIndex from %d to %d", raft.nodeID, oldCommitIndex, raft.commitIndex)
	}

	// 回复成功
	resp.Success = true
	resp.Term = raft.currentTerm
	log.Printf("Node %s sending AppendEntries reply: success=%t, term=%d", raft.nodeID, resp.Success, resp.Term)
	return resp, nil
}

// applyLoop 是一个无限循环，负责将已提交的日志应用到状态机
// 当有新的日志被提交时，会被唤醒执行应用操作
func (raft *Raft) applyLoop() {
	for {
		raft.mu.Lock()
		// 如果没有新的日志需要应用，等待信号
		for raft.lastApplied >= raft.commitIndex {
			raft.applyCond.Wait()
		}

		// 如果 lastApplied 落后于快照的索引，直接更新到快照索引
		if raft.lastApplied < raft.logMgr.lastIncludedIndex {
			raft.lastApplied = raft.logMgr.lastIncludedIndex
			raft.mu.Unlock()
			continue
		}

		// 保存当前的提交索引和已应用索引，避免在解锁后被修改
		commitIndex := raft.commitIndex
		lastApplied := raft.lastApplied

		// 收集需要应用的日志条目
		var entries []LogEntry
		for i := lastApplied + 1; i <= commitIndex; i++ {
			// 获取日志条目的物理索引
			pIdx := raft.logMgr.getPhysicalIndex(i)
			if pIdx >= 0 && pIdx < len(raft.logMgr.entries) {
				entries = append(entries, raft.logMgr.entries[pIdx])
			}
		}
		raft.mu.Unlock()

		// 记录应用日志
		if len(entries) > 0 {
			log.Printf("Node %s applying %d log entries from index %d to %d", raft.nodeID, len(entries), lastApplied+1, commitIndex)
		}

		// 将日志条目应用到状态机
		for _, entry := range entries {
			applyMsg := &ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: int(entry.Index),
			}
			raft.applyCh <- applyMsg

			// 检查是否有等待该日志索引的结果 channel
			raft.mu.Lock()
			if resultCh, exists := raft.proposalMap[entry.Index]; exists {
				// 发送结果到 channel
				select {
				case resultCh <- applyMsg:
					// 成功发送结果
					log.Printf("Node %s sent result for log index %d to proposal channel", raft.nodeID, entry.Index)
				default:
					// channel 已满或已关闭，记录错误
					log.Printf("Node %s failed to send result for log index %d: channel full or closed", raft.nodeID, entry.Index)
				}
				// 删除映射，避免内存泄漏
				delete(raft.proposalMap, entry.Index)
			}
			raft.mu.Unlock()
		}

		// 更新已应用索引
		raft.mu.Lock()
		// 确保 lastApplied 不会超过当前的 commitIndex
		oldLastApplied := raft.lastApplied
		raft.lastApplied = max(raft.lastApplied, commitIndex)
		if raft.lastApplied > oldLastApplied {
			log.Printf("Node %s updated lastApplied from %d to %d", raft.nodeID, oldLastApplied, raft.lastApplied)
		}
		raft.mu.Unlock()
	}
}

// encodeState 将 Raft 的状态序列化为字节数组
// 用于持久化存储
func (raft *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// 序列化当前任期
	e.Encode(raft.currentTerm)
	// 序列化投票给的节点 ID
	e.Encode(raft.votedFor)
	// 序列化日志条目
	e.Encode(raft.logMgr.entries)
	// 序列化快照的最后索引
	e.Encode(raft.logMgr.lastIncludedIndex)
	// 序列化快照的最后任期
	e.Encode(raft.logMgr.lastIncludedTerm)
	return w.Bytes()
}

// decodeState 将字节数组反序列化为 Raft 的状态
// 用于从持久化存储中恢复状态
func (raft *Raft) decodeState(data []byte) {
	// 如果数据为空，直接返回
	if len(data) == 0 {
		return
	}

	// 初始化解码器
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// 声明需要反序列化的变量
	var term int64
	var votedFor string
	var entries []LogEntry
	var lastIndex int64
	var lastTerm int64

	// 尝试反序列化所有字段
	if d.Decode(&term) == nil && d.Decode(&votedFor) == nil &&
		d.Decode(&entries) == nil && d.Decode(&lastIndex) == nil && d.Decode(&lastTerm) == nil {
		// 反序列化成功，更新状态
		raft.currentTerm = term
		raft.votedFor = votedFor
		raft.logMgr.entries = entries
		raft.logMgr.lastIncludedIndex = lastIndex
		raft.logMgr.lastIncludedTerm = lastTerm
	}
}

// resetElectionTimer 重置选举计时器
// 为计时器设置一个新的随机超时时间（1-2秒）
func (raft *Raft) resetElectionTimer() {
	// 尝试停止计时器
	if !raft.electionTimer.Stop() {
		// 如果计时器已经触发，尝试消费通道中的值
		select {
		case <-raft.electionTimer.C:
		default:
		}
	}
	// 重置计时器，设置一个新的随机超时时间（1-2秒）
	raft.electionTimer.Reset(tools.RandDuration(1000, 2000))
}

// killed 检查节点是否已关闭
// 目前总是返回 false，预留接口用于后续扩展
func (raft *Raft) killed() bool { return false }
