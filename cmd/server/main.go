package main

import (
	"fmt"
	"raft_kv/internal/server"
)

func main() {
	s := server.NewServer(":8080")
	fmt.Println("Server is running on :8080")
	if err := s.RunHttp(); err != nil {
		panic(err)
	}
}
