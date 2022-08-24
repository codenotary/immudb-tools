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
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"log"
	"os"
	"os/exec"
	"sync/atomic"
	"time"
)

var config struct {
	Seed      int
	IpAddr    string
	Port      int
	Username  string
	Password  string
	DBName    string
	ImmudbCmd string
	BatchSize int
	Workers   int
	Duration  int
	Interval  int
	Rest      int
	StorFile  string
	Op        string
}

func init() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.StringVar(&config.ImmudbCmd, "immudb", "immudb", "full immudb binary path")
	flag.StringVar(&config.Op, "operation", "write", "Operation { read | write }")
	flag.StringVar(&config.StorFile, "statefile", "stor.txt", "State file")
	flag.IntVar(&config.BatchSize, "batchsize", 1000, "Batch size")
	flag.IntVar(&config.Workers, "workers", 1, "Number of concurrent insertion processes")
	flag.IntVar(&config.Seed, "seed", 0, "Key seed")
	flag.IntVar(&config.Duration, "duration", 60, "total test duration (seconds)")
	flag.IntVar(&config.Interval, "interval", 30, "Single immudb run duration (seconds)")
	flag.IntVar(&config.Rest, "rest", 3, "Time between immudb restarts (seconds)")
	flag.Parse()
	startRnd(63)
}

func immudbRun(quit *int64) {
	for {
		q := atomic.LoadInt64(quit)
		if q != 0 {
			break
		}
		log.Printf("Starting immudb")
		cmd := exec.Command(config.ImmudbCmd)
		err := cmd.Start()
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Duration(config.Interval) * time.Second)
		log.Printf("Killing immudb")
		cmd.Process.Signal(os.Kill)
		time.Sleep(time.Duration(config.Rest) * time.Second)
	}
	log.Printf("Quitting immudb cycle")
}

func kvStore(c chan *schema.KeyValue) {
	log.Printf("Starting kvstore")
	f, err := os.OpenFile(config.StorFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Unable to open/create state file")
	}
	defer f.Close()
	for {
		kv := <-c
		if kv == nil {
			break
		}
		fmt.Fprintf(f, "%s %s\n", kv.Key, kv.Value)
	}
	log.Printf("Quitting kvstore")
}

func writeMain() {
	kvflow := make(chan *schema.KeyValue, 10)
	end := make(chan string)
	var quit int64
	quit = 0

	go func() {
		kvStore(kvflow)
		end <- "kvstore"
	}()

	go func() {
		immudbRun(&quit)
		end <- "immudb"
	}()
	// give some time to immudb to start
	time.Sleep(time.Duration(config.Rest) * time.Second)

	for i := 0; i < config.Workers; i++ {
		go func(c int) {
			for {
				q := atomic.LoadInt64(&quit)
				if q != 0 {
					log.Printf("Quitting worker %d cycle", c)
					break
				}
				writeWorker(c, kvflow)
				log.Printf("Worker %d died, sleeping and retrying", c)
				time.Sleep(1 * time.Second)
			}
			end <- fmt.Sprintf("Worker %d", c)
		}(i)
	}
	time.Sleep(time.Duration(config.Duration) * time.Second)
	log.Printf("Shutting down everything")
	atomic.StoreInt64(&quit, 1)
	for i := 0; i < config.Workers+2; i++ {
		s := <-end
		log.Printf("%s ended", s)
		if i == config.Workers+1 {
			kvflow <- nil
		}

	}
	log.Printf("Done")
}

func readMain() {
	log.Printf("Starting immudb")
	cmd := exec.Command(config.ImmudbCmd)
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Duration(config.Rest) * time.Second)
	readWorker()
	cmd.Process.Signal(os.Kill)
}

func main() {
	switch config.Op {
	case "read":
		readMain()
	case "write":
		writeMain()
	default:
		log.Fatal("Wrong operation specified")
	}
}
