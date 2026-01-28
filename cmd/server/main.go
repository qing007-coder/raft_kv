package main

import (
	"flag"
	"fmt"
	"raft_kv/internal/server"
)

func main() {
	// 解析命令行参数
	addr := flag.String("addr", ":8080", "HTTP server address")
	configPath := flag.String("config", "./config/node.yaml", "Configuration file path")
	flag.Parse()

	// 创建服务器实例
	s := server.NewServer(*addr, *configPath)
	fmt.Printf("Server is running on %s with config %s\n", *addr, *configPath)

	// 运行HTTP服务器
	if err := s.RunHttp(); err != nil {
		panic(err)
	}
}
