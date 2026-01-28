package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"raft_kv/internal/consensus"
)

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/kv", s.kvHandler)
}

func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handlePut(w, r)
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// PUT /kv
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req PutReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// 构造命令
	cmd := struct {
		Op    string `json:"op"`
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}{
		Op:    "put",
		Key:   req.Key,
		Value: []byte(req.Value),
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, "failed to marshal command", http.StatusInternalServerError)
		return
	}

	// 通过 Raft 提交命令
	resultCh := make(chan *consensus.ApplyMsg, 1)
	_, _, isLeader := s.raft.ProposeWithCallback(cmdBytes, resultCh)
	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	// 等待命令提交
	select {
	case <-resultCh:
		// 命令已提交并应用
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(PutResp{OK: true})
	case <-time.After(3 * time.Second):
		http.Error(w, "timeout waiting for command to be committed", http.StatusRequestTimeout)
		return
	}
}

// GET /kv?key=xxx
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	fmt.Println("key:", key)

	value, ok := s.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	fmt.Println("value:", value)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(GetResp{
		Key:   key,
		Value: string(value),
	})
}

// DELETE /kv?key=xxx
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// 构造命令
	cmd := struct {
		Op  string `json:"op"`
		Key string `json:"key"`
	}{
		Op:  "delete",
		Key: key,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		http.Error(w, "failed to marshal command", http.StatusInternalServerError)
		return
	}

	// 通过 Raft 提交命令
	resultCh := make(chan *consensus.ApplyMsg, 1)
	_, _, isLeader := s.raft.ProposeWithCallback(cmdBytes, resultCh)
	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	// 等待命令提交
	select {
	case <-resultCh:
		// 命令已提交并应用
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(DeleteResp{OK: true})
	case <-time.After(3 * time.Second):
		http.Error(w, "timeout waiting for command to be committed", http.StatusRequestTimeout)
		return
	}
}
