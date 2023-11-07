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

type Server struct {
	node     *maelstrom.Node
	counters map[string]float64
	mu       sync.Mutex
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
		node:     maelstromNode,
		counters: make(map[string]float64),
	}
	maelstromNode.Handle("add", server.handleAdd)
	maelstromNode.Handle("read", server.handleRead)
	maelstromNode.Handle("gossip", server.handleGossip)
	return server
}

func main() {
	server := newServer()
	ticker := time.NewTicker(time.Millisecond * 100)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Printf("gossip enter")
				wg := sync.WaitGroup{}
				wg.Add(len(server.node.NodeIDs()) - 1)
				for _, n := range server.node.NodeIDs() {
					dst := n
					log.Printf("entering node %s", dst)
					if dst == server.node.ID() {
						continue
					}
					go func() {
						defer wg.Done()
						answer, err := server.rpcWithRetry(dst, map[string]any{"type": "gossip"})
						if err != nil {
							return
						}
						var body map[string]any
						if err = json.Unmarshal(answer.Body, &body); err != nil {
							log.Printf("panic")
							panic("expected to be able to parse gossip reply body")
						}
						dstCount := body["value"].(float64)
						server.mu.Lock()
						server.counters[dst] = dstCount
						server.mu.Unlock()
					}()
				}
				wg.Wait()
			}
		}
	}()
	if err := server.node.Run(); err != nil {
		fmt.Printf("ERROR: %s", err)
		os.Exit(1)
	}
	// Execute the node's message loop. This will run until STDIN is closed.

}

func (server *Server) handleAdd(msg maelstrom.Message) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	delta := body["delta"].(float64)
	server.counters[server.node.ID()] += delta
	return server.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

func (server *Server) handleRead(msg maelstrom.Message) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	sum := 0.0
	for _, val := range server.counters {
		sum += val
	}
	return server.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": sum,
	})
}

func (server *Server) handleGossip(msg maelstrom.Message) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.node.Reply(msg, map[string]any{
		"type":  "gossip_ok",
		"value": server.counters[server.node.ID()],
	})
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
