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
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

var config struct {
	Seed           int
	MaxSeed        int
	IpAddr         string
	Port           int
	Username       string
	Password       string
	DBName         string
	RBatchNum      int
	WBatchNum      int
	WBatchSize     int
	RBatchSize     int
	RWorkers       int
	WWorkers       int
	WSpeed         int
	HashedKeys     bool
	RandomPayloads bool
}

func init() {
	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&config.RWorkers, "read-workers", 1, "Number of concurrent read processes")
	flag.IntVar(&config.WWorkers, "write-workers", 1, "Number of concurrent insertion processes")
	flag.IntVar(&config.RBatchNum, "read-batchnum", 5, "Number of read batches")
	flag.IntVar(&config.WBatchNum, "write-batchnum", 5, "Number of write batches")
	flag.IntVar(&config.RBatchSize, "read-batchsize", 1000, "Read batch size")
	flag.IntVar(&config.WBatchSize, "write-batchsize", 1000, "Write batch size")
	flag.IntVar(&config.WSpeed, "write-speed", 500, "Target write speed (KV writes per second). 0 to disable throttling")
	flag.IntVar(&config.Seed, "seed", 0, "Key seed start")
	flag.IntVar(&config.MaxSeed, "max-seed", 0, "Key seed max")
	flag.BoolVar(&config.HashedKeys, "hashed-keys", false, "Use sha356 digests as keys")
	flag.BoolVar(&config.RandomPayloads, "random-payloads", false, "Use random payloads when writing")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	log.Printf("Running on database: %s, workers: %d/%d, batchnum: %d/%d, batchsize: %d/%d.\n",
		config.DBName, config.RWorkers, config.WWorkers, config.RBatchNum, config.WBatchNum, config.RBatchSize, config.WBatchSize)
	end := make(chan bool)
	startRnd(64)

	var totalReads, totalWrites int64

	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		var estSpeedR float64
		var estSpeedW float64

		var lastCounterR, lastCounterW int64

		t0 := time.Now()
		t1 := t0

		for {
			select {
			case <-done:
				end <- true
				counterR := atomic.LoadInt64(&totalReads)
				counterW := atomic.LoadInt64(&totalWrites)
				t1 = time.Now()
				log.Printf("DONE in %s: read %d entries, %f KV/s, wrote %d entries, %f KV/s",
					time.Since(t0),
					counterR,
					float64(counterR)/float64(t1.Sub(t0).Seconds()),
					counterW,
					float64(counterW)/float64(t1.Sub(t0).Seconds()),
				)
				return

			case <-ticker.C:
				var avgSpeedR, lastSpeedR float64
				var avgSpeedW, lastSpeedW float64
				deltat := time.Since(t1)
				t1 = time.Now()

				counterR := atomic.LoadInt64(&totalReads)
				counterW := atomic.LoadInt64(&totalWrites)

				if counterR > 0 {
					avgSpeedR = float64(counterR) / t1.Sub(t0).Seconds()
					if deltat.Milliseconds() > 0 {
						lastSpeedR = float64(counterR-lastCounterR) / deltat.Seconds()
						estSpeedR = 0.9*estSpeedR + 0.1*lastSpeedR
					}
					lastCounterR = counterR
				}
				if counterW > 0 {
					avgSpeedW = float64(counterW) / t1.Sub(t0).Seconds()
					if deltat.Milliseconds() > 0 {
						lastSpeedW = float64(counterW-lastCounterW) / deltat.Seconds()
						estSpeedW = 0.9*estSpeedW + 0.1*lastSpeedW
					}
					lastCounterW = counterW
				}
				log.Printf(
					"Read Speed: estimated %10.3f, instant %10.3f, average %10.3f (KV/sec), "+
						"Write Speed: estimated %10.3f, instant %10.3f, average %10.3f (KV/sec)",
					estSpeedR, lastSpeedR, avgSpeedR,
					estSpeedW, lastSpeedW, avgSpeedW,
				)
			}
		}

	}()

	var total_read_count, total_write_count int64
	var total_read_time, total_write_time float64

	for i := 0; i < config.WWorkers; i++ {
		go func(c int) {
			n, t := writeWorker(c+1, &totalWrites)
			total_write_count += n
			total_write_time += t
			end <- true
		}(i)
	}

	// A tiny delay to ensure some entries are already written
	time.Sleep(time.Millisecond * 10)
	for i := 0; i < config.RWorkers; i++ {
		go func(c int) {
			n, t := readWorker(c+1, &totalReads)
			total_read_count += n
			total_read_time += t
			end <- true
		}(i)
	}

	for i := 0; i < config.RWorkers+config.WWorkers; i++ {
		<-end
	}

	close(done)

	<-end

	if config.RWorkers > 0 && total_read_time != 0 {
		var r_speed float64
		r_speed = float64(total_read_count) / float64(total_read_time)
		r_speed = r_speed * float64(config.RWorkers)
		log.Printf("TOTAL READ: %d KV, speed %d read requests, %d KV/s", total_read_count, config.RBatchNum*config.RWorkers, int(math.Round(r_speed)))
	}

	if config.WWorkers > 0 && total_write_time != 0 {
		var w_speed float64
		w_speed = float64(total_write_count) / float64(total_write_time)
		w_speed = w_speed * float64(config.WWorkers)
		log.Printf("TOTAL WRITE: %d KV, speed %d transactions, %d KV/s", total_write_count, config.WBatchNum*config.WWorkers, int(math.Round(w_speed)))
	}
}
