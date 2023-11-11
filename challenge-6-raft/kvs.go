package main

import (
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sync"
)

type Kvs struct {
	data map[any]any
	mu   sync.Mutex
}

func newKvs() *Kvs {
	return &Kvs{
		data: make(map[any]any),
	}
}

func (kvs *Kvs) write(key any, value any) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.data[key] = value
	return nil
}

func (kvs *Kvs) read(key any) (any, error) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	val, found := kvs.data[key]
	if !found {
		return struct{}{}, errors.New(maelstrom.ErrorCodeText(20)) //key not found
	}
	return val, nil
}

func (kvs *Kvs) cas(key any, previousValue any, newValue any) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	val, found := kvs.data[key]
	if !found {
		return errors.New(maelstrom.ErrorCodeText(20)) //key not found
	}
	if val != previousValue {
		return errors.New(maelstrom.ErrorCodeText(22)) //precondition failed
	}
	kvs.data[key] = newValue
	return nil
}
