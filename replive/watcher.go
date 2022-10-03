/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

type (
	Watcher struct {
		clients []*StateClient
	}

	State struct {
		id     int
		Name   string
		IState *schema.ImmutableState
		Err    error
	}

	StateClient struct {
		id     int
		name   string
		db     string
		client client.ImmuClient
	}

	StateLoader interface {
		Load() []*State
	}

	Controller interface {
		Render([]*State)
		Resize()
	}
)

func NewWatcher(clients []*StateClient) *Watcher {
	return &Watcher{
		clients: clients,
	}
}

func (w *Watcher) Load() []*State {
	res := make([]*State, 0, len(w.clients))

	resCh := make(chan *State, len(w.clients))
	wg := sync.WaitGroup{}
	for _, c := range w.clients {
		wg.Add(1)
		go func(cli *StateClient) {
			defer wg.Done()
			state, err := cli.Load()
			resCh <- &State{IState: state, Err: err, Name: cli.name, id: cli.id}
		}(c)
	}
	wg.Wait()
	close(resCh)

	for st := range resCh {
		res = append(res, st)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].id < res[j].id
	})

	return res
}

func NewStateClient(id int, name string, url string, port int, db string) (*StateClient, error) {
	opts := client.
		DefaultOptions().
		WithAddress(url).
		WithPort(port)

	c := client.NewClient().WithOptions(opts)
	err := c.OpenSession(
		context.Background(),
		[]byte(`immudb`),
		[]byte(`immudb`),
		db,
	)
	if err != nil {
		return nil, err
	}
	return &StateClient{
		id:     id,
		name:   name,
		client: c,
		db:     db,
	}, nil
}

func (p *StateClient) reconnect() error {
	return p.client.OpenSession(
		context.Background(),
		[]byte(`immudb`),
		[]byte(`immudb`),
		p.db,
	)
}

func (p *StateClient) Load() (*schema.ImmutableState, error) {
	s, err := p.client.CurrentState(context.Background())

	// reconnect if connection is broken
	if errors.Is(err, client.ErrNotConnected) {
		err = p.reconnect()
	}

	return s, err
}
