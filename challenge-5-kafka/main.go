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
	rpcTimeoutInterval = time.Millisecond * 500
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
		log.Errorf("Failed to unmarshall send request from node %s to node %s. error: %s", msg.Src, msg.Dest, err)
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
			log.Errorf("Failed to send from node %s to node %s. error: %s", server.node.ID(), targetNode, err)
			return err
		}
		log.Errorf("Successfully sent from node %s to node %s. error: %s", server.node.ID(), targetNode, err)
		var resBody map[string]any
		if err = json.Unmarshal(reply.Body, &resBody); err != nil {
			log.Errorf("Failed to unmarshall send response from node %s to node %s. error: %s", server.node.ID(), targetNode, err)
			return err
		}
		offSet = int(resBody["offset"].(float64))
	} else {
		server.mu.Lock()

		nextOffset := server.latestOffsets[body.Key]
		if len(server.logs[body.Key]) > 0 {
			nextOffset += 1
		}

		server.logs[body.Key] = append(server.logs[body.Key], Entry{offset: nextOffset, message: body.Msg})
		server.latestOffsets[body.Key] = nextOffset
		offSet = nextOffset
		server.mu.Unlock()
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
		log.Errorf("Failed to unmarshall poll reuqest from node %s to node %s. error: %s", msg.Src, msg.Dest, err)
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
				log.Errorf("Failed to poll from node %s to node %s. error: %s", server.node.ID(), node, err)
				return err
			}
			log.Errorf("Successfully pollied from node %s to node %s. error: %s", server.node.ID(), node, err)
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
		log.Errorf("Failed to unmarshall list request from node %s to node %s. error: %s", msg.Src, msg.Dest, err)
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
			for _, key := range keys {
				_, found := server.committedOffsets[key]
				if !found {
					continue
				}
				answer[key] = server.committedOffsets[key]
			}
		} else {
			reply, err := server.rpc(node, map[string]any{
				"type": "list_committed_offsets",
				"keys": keys,
			})
			if err != nil {
				log.Errorf("Failed to fetch remote commited offsets from node %s to node %s. error: %s", server.node.ID(), node, err)
				return err
			}
			log.Errorf("Successfully fetched commited offsets from node %s to node %s. error: %s", server.node.ID(), node, err)
			var resBody ListCommitedOffsetsResponse
			if err = json.Unmarshal(reply.Body, &resBody); err != nil {
				log.Errorf("Failed to unmarshall commited offsets response from node %s to node %s. error: %s", server.node.ID(), node, err)
				return err
			}
			for key, offSet := range resBody.Offsets {
				answer[key] = offSet
			}
			log.Errorf("Successfully fetched remote commited offsets from node %s to node %s", server.node.ID(), node)
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
		log.Errorf("Failed to unmarshall commited offsets request from node %s to node %s. error: %s", msg.Src, msg.Dest, err)
		return err
	}

	offSetsSplitByNode := SplitDataByNodeId(body.Offsets, len(server.node.NodeIDs()))

	for node, commitedOffSets := range offSetsSplitByNode {
		if node != server.node.ID() {
			_, err := server.rpc(node, map[string]any{
				"type": "commit_offsets",
				"keys": commitedOffSets,
			})
			if err != nil {
				log.Errorf("Failed to commit offsets from node %s to node %s. error: %s", server.node.ID(), node, err)
				return err
			}
			log.Errorf("Successfully commited offsets from node %s to node %s. error: %s", server.node.ID(), node, err)
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

func (server *Server) rpc(dst string, msg map[string]any) (maelstrom.Message, error) {
	return server.node.SyncRPC(context.Background(), dst, msg)
}
