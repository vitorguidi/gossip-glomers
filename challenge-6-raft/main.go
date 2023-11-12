package main

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
	raftTransport := MaelstromTransport{node: maelstromNode}
	raft := NewRaft(&raftTransport)
	kvs := newKvs()
	server := &Server{
		node: maelstromNode,
		raft: raft,
		kvs:  kvs,
	}

	maelstromNode.Handle("init", server.handleInit) //we only know node id after init msg, must tell raft after
	maelstromNode.Handle("read", server.handleRead)
	maelstromNode.Handle("write", server.handleWrite)
	maelstromNode.Handle("cas", server.handleCAS)
	maelstromNode.Handle("request_vote", server.handleRequestVote)
	maelstromNode.Handle("append_entries", server.handleAppendEntries)
	return server
}

func (s *Server) forward(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		//log.Errorf("Failed to deserialize message in the server.forward method")
		return err
	}
	if lastKnownLeader == "" {
		//log.Errorf("Tried to forward a %s message from %s to %s to a leader, but failed: no known leader",
		//	reqBody["type"], msg.Src, msg.Dest)
		return errors.New("unknown leader")
	}
	reply, err := s.node.SyncRPC(context.Background(), lastKnownLeader, reqBody)
	if err != nil {
		//log.Errorf("Tried to send a sync rpc from %s to last known leader %s, but failed: %s",
		//	msg.Src, msg.Dest, err)
		return err
	}
	return s.node.Reply(msg, reply)
}

func (s *Server) handleInit(_ maelstrom.Message) error {
	peers := s.node.NodeIDs()
	s.raft.nodeId = s.node.ID()
	s.raft.lastKnownLeader = ""
	s.raft.state = follower
	s.raft.peers = peers
	s.raft.commitedIndex = make([]int, len(peers))
	s.raft.nextIndex = make([]int, len(peers))
	s.raft.loop()
	return nil
}

func (s *Server) handleRead(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	if lastKnownLeader == "" || lastKnownLeader != s.node.ID() {
		return s.forward(msg)
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}
	key := reqBody["key"]
	val, err := s.kvs.read(key)
	if err != nil {
		return err
	}
	return s.node.Reply(msg, map[string]any{
		"value": val,
		"type":  "read_ok",
	})
}

func (s *Server) handleWrite(msg maelstrom.Message) error {
	lastKnownLeader := s.raft.lastKnownLeader
	if lastKnownLeader == "" || lastKnownLeader != s.node.ID() {
		//log.Errorf("forwarding write message from node %s to last leader = %s",
		//	s.raft.nodeId, s.raft.lastKnownLeader)
		return s.forward(msg)
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		log.Errorf("failed to deserialize message in the handleWrite method")
		return err
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
	if lastKnownLeader == "" || lastKnownLeader != s.node.ID() {
		//log.Errorf("forwarding cas message from node %s to last leader = %s",
		//	s.raft.nodeId, s.raft.lastKnownLeader)
		return s.forward(msg)
	}
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		log.Errorf("failed to deserialize message in the handleCAS method")
		return err
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
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		log.Errorf("failed to deserialize message in the handleRequestVote method")
		return err
	}
	log.Errorf("Received request vote request at node %s from node %s, content: %s", msg.Dest, msg.Src, reqBody)
	respBody := s.raft.handleRequestVote(mapToRequestVoteRequest(reqBody))
	log.Errorf("Node %s replying request vote of node %s, content: %s", msg.Dest, msg.Src, respBody)
	return s.node.Reply(msg, requestVoteResponseToMap(respBody))
}

func (s *Server) handleAppendEntries(msg maelstrom.Message) error {
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		log.Errorf("failed to deserialize message in the handleAppendEntries method")
		return err
	}
	log.Errorf("Received append entries request at node %s from node %s, content: %s", msg.Dest, msg.Src, reqBody)
	respBody := s.raft.handleAppendEntries(mapToAppendEntriesRequest(reqBody))
	//this works, receive end in raft does not
	log.Errorf("Node %s replying append entries request of node %s, content: %s", msg.Dest, msg.Src, respBody)
	return s.node.Reply(msg, appendEntriesResponseToMap(respBody))
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
	ctx, _ := context.WithTimeout(context.Background(), rpcTimeout)
	reply, err := s.node.SyncRPC(ctx, dst, body)
	if err != nil {
		log.Errorf("failed to send sync rpc from %s to %s of type %s message in the rpc method: %s",
			s.node.ID(), dst, body["type"], err)
		return nil, err
	}
	var resBody map[string]any
	if err = json.Unmarshal(reply.Body, &resBody); err != nil {
		log.Errorf("failed to deserialize message in the rpc method")
		return nil, err
	}
	return resBody, nil
}
