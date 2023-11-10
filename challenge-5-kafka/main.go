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

	targetNode, _ := GetNodeIdFromKey(body.Key, len(server.node.NodeIDs()))

	var offSet int

	if targetNode != server.node.ID() {
		reply, err := server.rpc(targetNode, map[string]any{
			"msg":  body.Msg,
			"type": "send",
			"key":  body.Key,
		})
		if err != nil {
			return err
		}
		var resBody map[string]any
		if err = json.Unmarshal(reply.Body, &resBody); err != nil {
			return err
		}
		offSet = resBody["offset"].(int)
	} else {
		server.mu.Lock()
		defer server.mu.Unlock()

		nextOffset := server.latestOffsets[body.Key]
		if len(server.logs[body.Key]) > 0 {
			nextOffset += 1
		}

		server.logs[body.Key] = append(server.logs[body.Key], Entry{offset: nextOffset, message: body.Msg})
		server.latestOffsets[body.Key] = nextOffset
		offSet = nextOffset
	}

	return server.node.Reply(msg, SendReply{Type: "send_ok", Offset: offSet})
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

	keysSplitByNode := SplitDataByNodeId(body.Offsets, len(server.node.NodeIDs()))
	answer := make(map[string][][2]int)

	for node, keysAndOffset := range keysSplitByNode {
		if node == server.node.ID() {
			server.mu.Lock()
			for key, offset := range keysAndOffset {
				answer[key] = getEntriesFromOffSet(server.logs[key], offset)
			}
			server.mu.Unlock()
		} else {
			reply, err := server.rpc(node, map[string]any{
				"type":    "poll",
				"offsets": keysAndOffset,
			})
			if err != nil {
				return err
			}
			var resBody PollReply
			if err = json.Unmarshal(reply.Body, &resBody); err != nil {
				return err
			}
			for key, entries := range resBody.Messages {
				for _, entry := range entries {
					answer[key] = append(answer[key], entry)
				}
			}
		}
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

	keyOffSetsByNode := make(map[string][]string)
	answer := make(map[string]int)

	for _, key := range body.Keys {
		targetNode, _ := GetNodeIdFromKey(key, len(server.node.NodeIDs()))
		_, found := keyOffSetsByNode[targetNode]
		if !found {
			keyOffSetsByNode[targetNode] = make([]string, 0)
		}
		keyOffSetsByNode[targetNode] = append(keyOffSetsByNode[targetNode], key)
	}

	for node, keys := range keyOffSetsByNode {
		if node == server.node.ID() {
			server.mu.Lock()
			for _, key := range keys {
				answer[key] = server.committedOffsets[key]
			}
			server.mu.Unlock()
		} else {
			reply, err := server.rpc(node, map[string]any{
				"type":    "list_commited_offsets",
				"offsets": keys,
			})
			if err != nil {
				return err
			}
			var resBody ListCommitedOffsetsResponse
			if err = json.Unmarshal(reply.Body, &resBody); err != nil {
				return err
			}
			for key, offSet := range resBody.Offsets {
				answer[key] = offSet
			}
		}
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

	offSetsSplitByNode := SplitDataByNodeId(body.Offsets, len(server.node.NodeIDs()))

	for node, commitedOffSets := range offSetsSplitByNode {
		if node != server.node.ID() {
			_, err := server.rpc(node, map[string]any{
				"type":    "commit_offsets",
				"offsets": commitedOffSets,
			})
			if err != nil {
				return err
			}
			//nothing fancy to do here, if we succeed we know all is gucci on the downstream
		} else {
			server.mu.Lock()
			for key, val := range body.Offsets {
				if val > server.committedOffsets[key] {
					server.committedOffsets[key] = val
				}
			}
			server.mu.Unlock()
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
