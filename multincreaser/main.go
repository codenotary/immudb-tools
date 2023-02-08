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
	IpAddr   string
	Port     int
	Username string
	Password string
	DBName   string
	Workers  int
	WorkSize int
	KeySpace int
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.Workers, "workers", 1, "Number of concurrent processes")
	flag.IntVar(&config.WorkSize, "worksize", 10, "Number of read/write iterations")
	flag.IntVar(&config.KeySpace, "keyspace", 10, "Number of different keys to operate with")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Printf("Starting with %d workers", config.Workers)
	var totalWrites int64
	end := make(chan bool)
	kspace := NewKeyspace(config.KeySpace)
	InitDB(kspace)
	for i := 0; i < config.Workers; i++ {
		go func(c int) {
			Worker(c+1, &totalWrites, kspace)
			end <- true
		}(i)
	}
	for i := 0; i < config.Workers; i++ {
		<-end
	}
	totalReaded := TotalDB(kspace)
	log.Printf("Work ended. Total written: %d Total Read: %d", totalWrites, totalReaded)
}
