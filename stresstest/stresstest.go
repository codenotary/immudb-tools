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
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"runtime"
)

var config struct {
	APIKey    string
	IpAddr    string
	Port      int
	Username  string
	Password  string
	DBName    string
	BatchNum  int
	BatchSize int
	Workers   int
}

func init() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.BatchNum, "batchnum", 5, "Number of batches")
	flag.IntVar(&config.BatchSize, "batchsize", 1000, "Batch size")
	flag.IntVar(&config.Workers, "workers", 1, "Concurrent insertions")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Printf("Running on database: %s, using batchnum: %d, batchsize: %d and workers: %d.\n",
		config.DBName, config.BatchNum, config.BatchSize, config.Workers)
	end := make(chan bool)
	startRnd(64, 2*config.BatchNum*config.BatchSize*config.Workers)
	for i := 0; i < config.Workers; i++ {
		go func(c int) {
			work(c + 1)
			end <- true
		}(i)
	}
	for i := 0; i < config.Workers; i++ {
		<-end
	}
}

func work(tnum int) {
	tokenfile := "token-" + string(getRnd()[:8])
	log.Println("Tokenfile: ", tokenfile)
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port).WithTokenFileName(tokenfile)
	client := immuclient.NewClient()
	client.SetupDialOptions(opts)
	ctx := context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}


	login, err := client.Login(ctx, []byte(config.Username), []byte(config.Password))
	if err != nil {
		log.Fatalln("Failed to login. Reason:", err.Error())
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))

	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: config.DBName})
	if err != nil {
		client.CloseSession(ctx)
		log.Fatalln("Failed to use the database. Reason:", err)
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	defer func() { log.Printf("Quitting %d",tnum); client.CloseSession(ctx) }()

	var counter uint64
	starttime := time.Now()

	for i := 0; i < config.BatchNum; i++ {
		log.Printf("T%d inserting batch %d ...", tnum, i+1)

		kvs := make([]*schema.KeyValue, config.BatchSize)

		for j := 0; j < config.BatchSize; j++ {
			kvs[j] = &schema.KeyValue{
				Key:   getRnd(),
				Value: getRnd(),
			}
		}

		kvList := &schema.SetRequest{KVs: kvs}
		if _, err = client.SetAll(ctx, kvList); err != nil {
			log.Fatalln("Failed to submit the batch. Reason:", err)
		} else {
			counter += uint64(config.BatchSize)
		}
	}
	log.Println("DONE: inserted", counter, "entries in", time.Since(starttime))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var rndChan chan []byte

func randStringBytesMaskImprSrcUnsafe(n int) {
	b := make([]byte, n)
	// A Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	rndChan <- b
}

func getRnd() []byte {
	ret := <-rndChan
	return ret
}

func startRnd(size, vals int) {
	rndChan = make(chan []byte, 65536)
	cpu := runtime.NumCPU()
	for j := 0; j < cpu; j++ {
		go func() {
			for i := 0; i < vals; i += cpu {
				randStringBytesMaskImprSrcUnsafe(size)
			}
		}()
	}
}

