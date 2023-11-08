package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

const (
	gossipInterval     = time.Millisecond * 50
	rpcTimeoutInterval = time.Millisecond * 150
	rpcRetries         = 3
)

type Entry struct {
	offset  int
	message int
}

type Server struct {
	node             *maelstrom.Node
	logs             map[string][]Entry
	committedOffsets map[string]int
	latestOffsets    map[string]int
	mu               sync.Mutex
}

func init() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
}

func newServer() *Server {
	var maelstromNode = maelstrom.NewNode()
	server := &Server{
		node:             maelstromNode,
		logs:             make(map[string][]Entry),
		committedOffsets: make(map[string]int),
		latestOffsets:    make(map[string]int),
	}
	maelstromNode.Handle("send", server.handleSend)
	maelstromNode.Handle("poll", server.handlePoll)
	maelstromNode.Handle("commit_offsets", server.handleCommitOffsets)
	maelstromNode.Handle("list_committed_offsets", server.handleListCommitedOffsets)
	return server
}

func main() {
	server := newServer()

	if err := server.node.Run(); err != nil {
		fmt.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	// Execute the node's message loop. This will run until STDIN is closed.

}

type SendRequest struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type SendReply struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

func (server *Server) handleSend(msg maelstrom.Message) error {
	var body SendRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.mu.Lock()
	defer server.mu.Unlock()

	nextOffset := server.latestOffsets[body.Key]
	if len(server.logs[body.Key]) > 0 {
		nextOffset += 1
	}

	server.logs[body.Key] = append(server.logs[body.Key], Entry{offset: nextOffset, message: body.Msg})
	server.latestOffsets[body.Key] = nextOffset

	log.Printf("Setting offset in key %s to %i", body.Key, nextOffset)

	return server.node.Reply(msg, SendReply{Type: "send_ok", Offset: nextOffset})
}

type PollRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollReply struct {
	Type     string              `json:"type"`
	Messages map[string][][2]int `json:"msgs"`
}

func (server *Server) handlePoll(msg maelstrom.Message) error {
	var body PollRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	answer := make(map[string][][2]int)
	for key := range body.Offsets {
		answer[key] = getEntriesFromOffSet(server.logs[key], body.Offsets[key])
	}
	return server.node.Reply(msg, PollReply{Type: "poll_ok", Messages: answer})
}

func getEntriesFromOffSet(entries []Entry, offSetStart int) [][2]int {
	answer := make([][2]int, 0)

	for i := offSetStart; i < len(entries) && i < offSetStart+10; i += 1 {
		answer = append(answer, [2]int{entries[i].offset, entries[i].message})
	}
	return answer
}

type ListCommitedOffsetsRequest struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommitedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func (server *Server) handleListCommitedOffsets(msg maelstrom.Message) error {
	var body ListCommitedOffsetsRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	answer := make(map[string]int)
	for _, key := range body.Keys {
		answer[key] = server.committedOffsets[key]
	}
	return server.node.Reply(msg, ListCommitedOffsetsResponse{Offsets: answer, Type: "list_committed_offsets_ok"})
}

type CommitOffsetsRequest struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsResponse struct {
	Type string `json:"type"`
}

func (server *Server) handleCommitOffsets(msg maelstrom.Message) error {
	var body CommitOffsetsRequest
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	for key, val := range body.Offsets {
		if val > server.committedOffsets[key] {
			server.committedOffsets[key] = val
		}
	}
	return server.node.Reply(msg, CommitOffsetsResponse{Type: "commit_offsets_ok"})
}

func (server *Server) rpcWithRetry(dst string, msg map[string]any) (maelstrom.Message, error) {
	var answer maelstrom.Message
	var err error
	for i := 0; i < rpcRetries; i++ {
		answer, err = server.rpc(dst, msg)
		if err == nil {
			break
		}
	}
	return answer, err

}

func (server *Server) rpc(dst string, msg map[string]any) (maelstrom.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeoutInterval)
	defer cancel()
	return server.node.SyncRPC(ctx, dst, msg)
}
