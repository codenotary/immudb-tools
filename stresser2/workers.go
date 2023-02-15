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
	"crypto/sha256"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
)

func connect(jobid string) (context.Context, immuclient.ImmuClient) {
	tokenfile := "token-" + string(getRnd()[:8])
	log.Printf("Job %s Tokenfile: %s", jobid, tokenfile)
	log.Printf("Addr: %s", config.IpAddr)
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port).WithTokenFileName(tokenfile)
	client := immuclient.NewClient().WithOptions(opts)
	ctx := context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return ctx, client
}

func h256(b []byte) []byte {
	h := sha256.New()
	h.Write(b)
	return h.Sum(nil)
}

type keyTracker struct {
	start int
	max   int
	mx    sync.RWMutex
}

var KeyTracker *keyTracker

func init() {
	KeyTracker = genKeyTracker()
}

func (kt *keyTracker) getWKey() []byte {
	kt.mx.Lock()
	defer kt.mx.Unlock()
	kt.max++

	key := make([]byte, config.KeySize)
	binary.BigEndian.PutUint32(key, uint32(kt.max))

	return key
}

func (kt *keyTracker) getRKey() []byte {
	kt.mx.RLock()
	defer kt.mx.RUnlock()
	var k = kt.start
	if kt.max-kt.start > 0 {
		k += rand.Intn(kt.max - kt.start)
	}

	key := make([]byte, config.KeySize)
	binary.BigEndian.PutUint32(key, uint32(k))

	return key
}

func genKeyTracker() *keyTracker {
	return &keyTracker{
		start: config.Seed,
		max:   config.MaxSeed,
		mx:    sync.RWMutex{},
	}
}

func readWorker(n int, totalCounter *int64) (counter int64, elapsed float64) {
	jobid := fmt.Sprintf("RJOB%02d", n)

	ctx, client := connect(jobid)
	defer client.CloseSession(ctx)
	t0 := time.Now()
	for i := 0; i < config.RBatchNum; i++ {
		var err error
		if config.BatchSize == 1 {
			_, err = client.Get(ctx, h256(KeyTracker.getRKey()))
		} else {
			kList := make([][]byte, config.BatchSize)
			for j := 0; j < config.BatchSize; j++ {
				kList[j] = h256(KeyTracker.getRKey())
			}
			_, err = client.GetAll(ctx, kList)
		}

		if err != nil && !strings.Contains(err.Error(), "key not found") {
			log.Fatalln("Failed to read the batch. Reason:", err)
		} else {
			counter += int64(config.BatchSize)
			atomic.AddInt64(totalCounter, int64(config.BatchSize))
		}
	}
	elapsed=float64(time.Since(t0).Seconds())
	log.Printf("%s DONE: read %d entries in %.3fs, %.3f KV/s", jobid, counter, elapsed, float64(counter)/float64(time.Since(t0).Seconds()))
	return counter, elapsed
}

func writeWorker(n int, totalCounter *int64) (counter int64, elapsed float64) {
	jobid := fmt.Sprintf("WJOB%02d", n)

	ctx, client := connect(jobid)
	defer client.CloseSession(ctx)

	var t1 time.Time

	kvList := &schema.SetRequest{KVs: make([]*schema.KeyValue, config.BatchSize), NoWait: config.RWorkers == 0}

	t0 := time.Now()
	for i := 0; i < config.WBatchNum; i++ {
		t1 = time.Now()

		for j := 0; j < config.BatchSize; j++ {
			kvList.KVs[j] = &schema.KeyValue{
				Key:   h256(KeyTracker.getWKey()),
				Value: getPayload(256),
			}
		}

		if _, err := client.SetAll(ctx, kvList); err != nil {
			log.Fatalln("Failed to submit the batch. Reason:", err)
		} else {
			counter += int64(config.BatchSize)
			atomic.AddInt64(totalCounter, int64(config.BatchSize))
		}
		if config.WSpeed > 0 {
			dt := time.Since(t1)
			sleepTime := (1000.0 * float64(config.BatchSize) / float64(config.WSpeed)) - float64(dt.Milliseconds())
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}

	}
	elapsed=float64(time.Since(t0).Seconds())
	log.Printf("%s DONE: inserted %d entries in %.3fs, %.3f KV/s", jobid, counter, elapsed, float64(counter)/float64(time.Since(t0).Seconds()))
	return counter, elapsed
}
