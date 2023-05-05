/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"log"
	"sync/atomic"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
)

var retries uint64

func init() {
	retries = 0
}

type t_tx struct {
	name       string
	statements []string
	tx         immudb.Tx
	ctx        context.Context
	ic         immudb.ImmuClient
	txsize     int
	total      int
}

func MakeTx(ctx context.Context, client immudb.ImmuClient, name string, txsize int) *t_tx {
	return &t_tx{
		name:  name,
		tx:    nil,
		ctx:   ctx,
		ic:    client,
		txsize: txsize,
		total: 0,
	}
}

func (t *t_tx) Add(stm string) {
	t.statements = append(t.statements, stm)
	if len(t.statements) >= t.txsize {
		t.Commit()
	}
}

func (t *t_tx) Commit() {
	if len(t.statements) == 0 {
		return
	}
	var err error
	var i int
	for i = 0; i < 5; i++ {
		t.tx, err = t.ic.NewTx(t.ctx)
		// t.tx, err = t.ic.NewTx(t.ctx, immudb.UnsafeMVCC(), immudb.SnapshotMustIncludeTxID(0), immudb.SnapshotRenewalPeriod(0))
		if err != nil {
			log.Fatalf("[%s] Error while creating transaction: %s", t.name, err)
		}
		for _, s := range t.statements {
			err = t.tx.SQLExec(t.ctx, s, nil)
			if err != nil {
				log.Fatalf("[%s] Error: %s", t.name, err)
			}
		}
		log.Printf("[%s] Committing %d / %d", t.name, len(t.statements), t.total)
		_, err := t.tx.Commit(t.ctx)
		if err == nil {
			break
		}
		atomic.AddUint64(&retries, 1)
		log.Printf("[%s] Tx Error: %s, retrying", t.name, err)
		time.Sleep(time.Duration((i+1)*(i+1)*5) * time.Millisecond)
	}
	if err != nil {
		log.Fatal("[%s] TX error: %s, abort", t.name, err)
	}
	if i>0 {
		log.Printf("[%s] Committing %d / %d: success after %d attempts", t.name, len(t.statements), t.total, i)
	}
	t.total = t.total + len(t.statements)
	t.statements = []string{}
}
