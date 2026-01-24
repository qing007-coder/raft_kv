package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"raft_kv/internal/server"
)

type Client struct {
	url string
}

func NewClient(url string) *Client {
	return &Client{url}
}

func (c *Client) Get(key string) string {
	query := make(map[string]interface{})
	query["key"] = key
	data := c.query(query)

	return string(data)
}

func (c *Client) Put(key string, value string) {
	req := server.PutReq{
		Key:   key,
		Value: value,
	}

	resp := c.post(req)
	fmt.Println(string(resp))
}

func (c *Client) Delete(key string) {
	client := new(http.Client)
	url := fmt.Sprintf("%s?key=%s", c.url, key)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("创建请求失败: %s\n", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("请求发送失败: %s\n", err)
		return
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(respData))
}

func (c *Client) post(req interface{}) []byte {
	data, err := json.Marshal(&req)
	if err != nil {
		panic(err)
	}

	resp, err := http.Post(c.url, "application/json", bytes.NewBuffer(data))

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	response, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return response
}

func (c *Client) query(data map[string]interface{}) []byte {
	url := c.url
	for key, value := range data {
		url = fmt.Sprintf("%s?%s=%s", url, key, value)
	}

	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	return body
}
