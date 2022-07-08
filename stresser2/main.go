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
	"flag"
	"log"
	"math/rand"
	"time"
)

var config struct {
	Seed      int
	IpAddr    string
	Port      int
	Username  string
	Password  string
	DBName    string
	RBatchNum int
	WBatchNum int
	BatchSize int
	RWorkers  int
	WWorkers  int
	WSpeed    int
}

func init() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.WBatchNum, "write-batchnum", 5, "Number of write batches")
	flag.IntVar(&config.RBatchNum, "read-batchnum", 5, "Number of read batches")
	flag.IntVar(&config.BatchSize, "batchsize", 1000, "Batch size")
	flag.IntVar(&config.RWorkers, "read-workers", 1, "Number of concurrent read processes")
	flag.IntVar(&config.WWorkers, "write-workers", 1, "Number of concurrent insertion processes")
	flag.IntVar(&config.WSpeed, "write-speed", 500, "Target write speed (KV writes per second)")
	flag.IntVar(&config.Seed, "seed", -1, "Key seed (use -1 to use time-based seed)")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Printf("Running on database: %s, using batchnum: %d/d, batchsize: %d and workers: %d/%d.\n",
		config.DBName, config.RBatchNum, config.WBatchNum, config.BatchSize, config.RWorkers, config.WWorkers)
	end := make(chan bool)
	startRnd(64)
	for i := 0; i < config.RWorkers; i++ {
		go func(c int) {
			readWorker(c + 1)
			end <- true
		}(i)
	}
	for i := 0; i < config.WWorkers; i++ {
		go func(c int) {
			writeWorker(c + 1)
			end <- true
		}(i)
	}
	for i := 0; i < config.RWorkers+config.WWorkers; i++ {
		<-end
	}
}
