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
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"log"
	"math/rand"
	"sync"
	"time"
)

func connect(jobid string) (context.Context, immuclient.ImmuClient) {
	tokenfile := "token-" + string(getRnd()[:8])
	log.Printf("Job %s Tokenfile: %s", jobid, tokenfile)
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port).WithTokenFileName(tokenfile)
	client := immuclient.NewClient()
	client.SetupDialOptions(opts)
	ctx := context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return ctx, client
}

func h256(s string) []byte {
	h := sha256.New()
	h.Write([]byte(s))
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

func (kt *keyTracker) getWKey() string {
	kt.mx.Lock()
	defer kt.mx.Unlock()
	kt.max++
	return fmt.Sprintf("KEY:%10d", kt.max)
}

func (kt *keyTracker) getRKey() string {
	kt.mx.RLock()
	defer kt.mx.RUnlock()
	k := rand.Intn(kt.max-kt.start) + kt.start
	return fmt.Sprintf("KEY:%10d", k)
}

func genKeyTracker() *keyTracker {
	return &keyTracker{
		start: config.Seed,
		mx:    sync.RWMutex{},
	}
}

func readWorker(n int) {
	jobid := fmt.Sprintf("RJOB%02d", n)

	ctx, client := connect(jobid)
	defer client.CloseSession(ctx)
	var counter int64
	var estSpeed float64
	t0 := time.Now()
	t1 := time.Now()
	for i := 0; i < config.RBatchNum; i++ {
		var avgSpeed, lastSpeed float64
		deltat := time.Since(t1)
		t1 = time.Now()

		if counter > 0 {
			avgSpeed = float64(counter*1000) / float64(time.Since(t0).Milliseconds())
			lastSpeed = float64(config.BatchSize*1000) / float64(deltat.Milliseconds())
			estSpeed = 0.9*estSpeed + 0.1*lastSpeed
		}
		log.Printf("%s reading batch %04d. Speed: estimated %10.3f, instant %10.3f, average %10.3f (KV/sec)", jobid, i+1, avgSpeed, lastSpeed, estSpeed)

		kList := make([][]byte, config.BatchSize)
		for j := 0; j < config.BatchSize; j++ {
			kList[j] = h256(KeyTracker.getRKey())
		}

		if _, err := client.GetAll(ctx, kList); err != nil {
			log.Fatalln("Failed to read the batch. Reason:", err)
		} else {
			counter += int64(config.BatchSize)
		}
	}
	log.Printf("%s DONE: read %d entries in %s", jobid, counter, time.Since(t0))
}

func writeWorker(n int) {
	jobid := fmt.Sprintf("WJOB%02d", n)

	ctx, client := connect(jobid)
	defer client.CloseSession(ctx)
	var counter int64
	var estSpeed float64
	t0 := time.Now()
	t1 := time.Now()
	for i := 0; i < config.WBatchNum; i++ {
		var avgSpeed, lastSpeed float64
		deltat := time.Since(t1)
		t1 = time.Now()
		if counter > 0 {
			avgSpeed = float64(counter*1000) / float64(time.Since(t0).Milliseconds())
			lastSpeed = float64(config.BatchSize*1000) / float64(deltat.Milliseconds())
			estSpeed = 0.9*estSpeed + 0.1*lastSpeed
		}
		log.Printf("%s writing batch %04d. Speed: estimated %10.3f, instant %10.3f, average %10.3f (KV/sec)", jobid, i+1, avgSpeed, lastSpeed, estSpeed)

		kvs := make([]*schema.KeyValue, config.BatchSize)

		for j := 0; j < config.BatchSize; j++ {
			kvs[j] = &schema.KeyValue{
				Key:   h256(KeyTracker.getWKey()),
				Value: getPayload(256),
			}
		}

		kvList := &schema.SetRequest{KVs: kvs}
		if _, err := client.SetAll(ctx, kvList); err != nil {
			log.Fatalln("Failed to submit the batch. Reason:", err)
		} else {
			counter += int64(config.BatchSize)
		}
		dt := time.Since(t1)
		sleepTime := (1000.0 * float64(config.BatchSize) / float64(config.WSpeed)) - float64(dt.Milliseconds())
		// log.Printf("Need to sleep %f ms", sleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)

	}
	log.Printf("%s DONE: inserted %d entries in %s", jobid, counter, time.Since(t0))

}
