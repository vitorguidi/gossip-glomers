package main

import (
	"context"
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

/*
https://www.youtube.com/watch?v=77qpCahU3fo&t=298s&ab_channel=MartinKleppmann
gossip protocol, simple
Idea:
	* keep buffer of messages
	* keep broadcasting until all got received
	* select at max neighbourSpreadFactor nodes to gossip
	* once u get broadcast response, update the seen list
	* if message was successfully broadcasted, remove from buffer
*/

const (
	gossipInterval     = time.Millisecond * 50
	rpcTimeoutInterval = time.Millisecond * 150
	rpcRetries         = 3
)

type Server struct {
	node             *maelstrom.Node
	allNodes         []string
	messageBuffer    map[float64]struct{}
	receivedMessages []float64
	messagesMut      sync.Mutex
}

func newServer() *Server {
	var maelstromNode = maelstrom.NewNode()
	server := &Server{
		node:             maelstromNode,
		messageBuffer:    make(map[float64]struct{}),
		receivedMessages: make([]float64, 0),
		allNodes:         maelstromNode.NodeIDs(),
	}
	maelstromNode.Handle("read", server.handleRead)
	maelstromNode.Handle("topology", server.handleTopology)
	maelstromNode.Handle("broadcast", server.handleBroadcast)
	return server
}

func main() {
	server := newServer()
	go func() {
		select {
		case <-time.Tick(gossipInterval):
			server.batchBroadcast()
		}
	}()

	if err := server.node.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

	// Execute the node's message loop. This will run until STDIN is closed.

}

func (server *Server) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.messagesMut.Lock()
	defer server.messagesMut.Unlock()
	// Echo the original message back with the updated message type.
	return server.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": server.receivedMessages,
	})
}

func (server *Server) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	server.messagesMut.Lock()

	//handle cases separate => has message = jepsen bombarding us
	// has messages => me saving network bandwith by batching

	messageField, foundMessageField := body["message"]

	if foundMessageField && messageField != nil {
		msgId := body["message"].(float64)
		_, messageHasBeenReceivedBefore := server.messageBuffer[msgId]
		if !messageHasBeenReceivedBefore {
			server.receivedMessages = append(server.receivedMessages, msgId)
			server.messageBuffer[msgId] = struct{}{}
		}
	}

	messagesField, foundMessagesField := body["messages"]

	if foundMessagesField && messagesField != nil {
		msgList := body["messages"].([]any)
		for _, msgId := range msgList {
			_, messageHasBeenReceivedBefore := server.messageBuffer[msgId.(float64)]
			if !messageHasBeenReceivedBefore {
				server.receivedMessages = append(server.receivedMessages, msgId.(float64))
				server.messageBuffer[msgId.(float64)] = struct{}{}
			}
		}
	}

	server.messagesMut.Unlock()

	go func() {
		server.batchBroadcast()
	}()

	return server.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

// not using topology, we can assume that all nodes are connected and work from this
// as per problem description https://fly.io/dist-sys/3a/
func (server *Server) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	return server.node.Reply(msg, map[string]any{"type": "topology_ok"})
}

func (server *Server) batchBroadcast() {
	server.messagesMut.Lock()
	defer server.messagesMut.Unlock()
	wg := sync.WaitGroup{}
	neighbours := server.node.NodeIDs()
	msg := map[string]any{
		"type":     "broadcast",
		"messages": server.receivedMessages,
	}
	wg.Add(len(neighbours))
	for _, n := range neighbours {
		dst := n
		go func() {
			defer wg.Done()
			server.rpcWithRetry(dst, msg)
		}()
	}
	wg.Wait()
}

func (server *Server) rpcWithRetry(dst string, msg map[string]any) {
	for i := 0; i < rpcRetries; i++ {
		if err := server.rpc(dst, msg); err == nil {
			break
		}
	}
}

func (server *Server) rpc(dst string, msg map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeoutInterval)
	defer cancel()
	_, err := server.node.SyncRPC(ctx, dst, msg)
	return err
}
