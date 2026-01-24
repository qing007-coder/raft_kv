package main

import (
	"fmt"
	"raft_kv/test"
)

func main() {
	client := test.NewClient("http://localhost:8080/kv")
	client.Put("server_name", "gateway")

	data := client.Get("server_name")
	fmt.Println(data)

	client.Delete("server_name")
	data = client.Get("server_name")

	fmt.Println(data)
}
