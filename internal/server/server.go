package server

import (
	"net/http"
	"raft_kv/internal/store/memory"
)

type Server struct {
	addr  string
	store *memory.Store
	mux   *http.ServeMux
}

func NewServer(addr string) *Server {
	return &Server{
		addr:  addr,
		store: memory.NewStore(),
		mux:   http.NewServeMux(),
	}
}

func (s *Server) RunHttp() error {
	s.registerRoutes()
	return http.ListenAndServe(s.addr, s.mux)
}
