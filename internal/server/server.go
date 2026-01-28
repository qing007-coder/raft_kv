package server

import (
	"fmt"
	"net/http"
	"raft_kv/internal/consensus"
	"raft_kv/internal/store/memory"
)

type Server struct {
	addr  string
	raft  *consensus.Raft
	store *memory.Store
	mux   *http.ServeMux
}

func NewServer(addr string, configPath string) *Server {
	// 创建applyChan通道，用于Raft向状态机传递命令
	applyChan := make(chan *consensus.ApplyMsg, 100)

	// 创建存储实例
	store := memory.NewStore()

	// 启动存储服务，处理从applyChan来的消息
	store.Start(applyChan)

	// 创建Raft实例
	raft := consensus.NewRaft(applyChan, configPath)

	return &Server{
		addr:  addr,
		raft:  raft,
		store: store,
		mux:   http.NewServeMux(),
	}
}

func (s *Server) RunHttp() error {
	// 启动Raft共识模块
	go func() {
		fmt.Println("Starting Raft consensus module...")
		s.raft.Start()
	}()

	// 注册路由
	s.registerRoutes()

	// 启动HTTP服务器
	fmt.Printf("Server is running on %s\n", s.addr)
	return http.ListenAndServe(s.addr, s.mux)
}
