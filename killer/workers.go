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
	"bufio"
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"log"
	"os"
	"strings"
	"time"
)

func connect(jobid string) (context.Context, immuclient.ImmuClient, error) {
	tokenfile := "token-" + string(getRnd()[:8])
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port).WithTokenFileName(tokenfile)
	client := immuclient.NewClient().WithOptions(opts)
	ctx := context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Printf("Job %s failed to connect. Reason: %s", jobid, err)
		return ctx, client, err
	}
	return ctx, client, nil
}

func writeWorker(n int, store chan *schema.KeyValue) {
	jobid := fmt.Sprintf("WJOB%02d", n)
	log.Printf("Starting job %s", jobid)

	ctx, client, err := connect(jobid)
	if err != nil {
		return
	}
	defer client.CloseSession(ctx)
	defer client.Disconnect()
	log.Printf("Job %s inserting", jobid)
	for {

		kvs := make([]*schema.KeyValue, config.BatchSize)

		for j := 0; j < config.BatchSize; j++ {
			kvs[j] = &schema.KeyValue{
				Key:   getRnd(),
				Value: getRnd(),
			}
		}

		kvList := &schema.SetRequest{KVs: kvs, NoWait: true}
		if _, err := client.SetAll(ctx, kvList); err != nil {
			log.Printf("Failed to submit the batch. Reason: %s", err)
			return
		} else {
			for j := 0; j < config.BatchSize; j++ {
				store <- kvs[j]
			}
		}
	}
}

func readWorker() {
	log.Printf("Read worker starting")
	ctx, client, err := connect("-")
	if err != nil {
		log.Fatal("Unable to connect to immudb")
	}
	log.Printf("Read worker connected")
	defer client.CloseSession(ctx)
	f, err := os.Open(config.StorFile)
	if err != nil {
		log.Fatal("Unable to read stor.txt")
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	t0 := time.Now()
	count := 0
	error_count := 0
	kList := make([][]byte, config.BatchSize)
	vList := make([]string, config.BatchSize)
	kPtr := 0
	for scanner.Scan() {
		if (count&255 == 0) && (time.Since(t0) > time.Second) {
			log.Printf("Check: %d", count)
			t0 = time.Now()
		}
		kv := strings.Split(scanner.Text(), " ")
		kList[kPtr]=[]byte(kv[0])
		vList[kPtr]=kv[1]
		kPtr++
		count++
		if kPtr==config.BatchSize {
			entry, err := client.GetAll(ctx, kList)
			if err != nil {
				log.Printf("Unable to fetch key %s", kv[0])
				error_count++
			} else {
				for req_idx,ret_idx := 0,0; req_idx<kPtr; req_idx++ {
					if string(entry.Entries[ret_idx].Key) == string(kList[req_idx]) {
						if string(entry.Entries[ret_idx].Value)!=vList[req_idx] {
							log.Printf("Wrong value for key %s", string(kList[req_idx]))
							error_count++
						}
						ret_idx++
					} else {
						log.Printf("Key %s not found", string(kList[req_idx]))
						error_count++
					}
						
						
				}
			}
			kPtr=0

		}
	}
	log.Printf("Read worker ended. Read %d errors %d", count, error_count)
}
