package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
)

//initially without persistent, lets just handle network partitions
//after we get it working, lets think about persistence

type Server struct {
	node *maelstrom.Node
	raft *Raft
	kvs  *Kvs
	mu   sync.Mutex
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
	commitC := make(chan any)
	proposeC := make(chan any)
	raft := NewRaft(maelstromNode.ID(), commitC, proposeC, maelstromNode.NodeIDs())
	kvs := newKvs()
	server := &Server{
		node: maelstromNode,
		raft: raft,
		kvs:  kvs,
	}

	maelstromNode.Handle("read", server.handleRead)
	maelstromNode.Handle("write", server.handleWrite)
	maelstromNode.Handle("cas", server.handleCAS)
	maelstromNode.Handle("request_vote", server.handleRequestVote)
	maelstromNode.Handle("append_entries", server.handleAppendEntries)
	return server
}

func (s *Server) handleRead(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	if lastKnownLeader == "" {
		return errors.New("unknown leader")
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}
	if lastKnownLeader != s.node.ID() {
		reply, err := s.node.SyncRPC(context.Background(), lastKnownLeader, reqBody)
		if err != nil {
			return err
		}
		return s.node.Reply(msg, reply)
	}
	key := reqBody["key"]
	val, _ := s.kvs.read(key)
	return s.node.Reply(msg, map[string]any{
		"value": val,
		"type":  "read_ok",
	})
}

func (s *Server) handleWrite(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	if lastKnownLeader == "" {
		return errors.New("unknown leader")
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}
	if lastKnownLeader != s.node.ID() {
		reply, err := s.node.SyncRPC(context.Background(), lastKnownLeader, reqBody)
		if err != nil {
			return err
		}
		return s.node.Reply(msg, reply)
	}
	key := reqBody["key"]
	val := reqBody["value"]
	_ = s.kvs.write(key, val)

	return s.node.Reply(msg, map[string]any{
		"type": "write_ok",
	})
}

func (s *Server) handleCAS(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	if lastKnownLeader == "" {
		return errors.New("unknown leader")
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}
	if lastKnownLeader != s.node.ID() {
		reply, err := s.node.SyncRPC(context.Background(), lastKnownLeader, reqBody)
		if err != nil {
			return err
		}
		return s.node.Reply(msg, reply)
	}
	key := reqBody["key"]
	from := reqBody["from"]
	to := reqBody["to"]

	err := s.kvs.cas(key, from, to)
	if err != nil {
		return err
	}

	return s.node.Reply(msg, map[string]any{
		"type": "cas_ok",
	})
}

func (s *Server) handleRequestVote(msg maelstrom.Message) error {
	return nil
}

func (s *Server) handleAppendEntries(msg maelstrom.Message) error {
	return nil
}

func main() {
	server := newServer()

	if err := server.node.Run(); err != nil {
		fmt.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	// Execute the node's message loop. This will run until STDIN is closed.
}

func (s *Server) rpc(dst string, body map[string]any) (map[string]any, error) {
	reply, err := s.node.SyncRPC(context.Background(), dst, body)
	if err != nil {
		return nil, err
	}
	var resBody map[string]any
	if err = json.Unmarshal(reply.Body, &resBody); err != nil {
		return nil, err
	}
	return resBody, nil
}
