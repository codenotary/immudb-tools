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
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"
)

var config struct {
	Username          string
	Password          string
	IpAddr            string
	Port              int
	Delay             int
	Timeout           int
	Debug             bool
	MaxReqWait        int64
	ProgressThreshold int64
}

var (
	Version   = "0.0"
	Buildtime = "00"
	Commit    = "00"
	AESKey    = "NOKEY"
)

type status struct {
	mx sync.Mutex
	ok bool
}

var immudb_status status

func (s *status) fail() {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.ok = false
}

func (s *status) success() {
	s.mx.Lock()
	defer s.mx.Unlock()
	s.ok = true
}

func (s *status) get_status() bool {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.ok
}

func aesdecrypt(s string) (string, error) {
	bs, err := hex.DecodeString(s)
	if err != nil {
		return s, err
	}
	block, err := aes.NewCipher([]byte(AESKey))
	if err != nil {
		return s, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return s, err
	}
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := bs[:nonceSize], bs[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return s, err
	}
	return string(plaintext), nil
}

var debug *log.Logger

func init() {

	defaultPw := "immudb"
	envPw, ok := os.LookupEnv("IMMUDB_ADMIN_PASSWORD")
	if ok {
		defaultPw = envPw
	}

	flag.StringVar(&config.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", defaultPw, "Password for authenticating to immudb; overrides env variable IMMUDB_ADMIN_PASSWORD")
	flag.IntVar(&config.Delay, "delay", 60, "Delay between scans")
	flag.IntVar(&config.Timeout, "timeout", 5, "API timeout")
	flag.Int64Var(&config.MaxReqWait, "reqtimeout", 60000, "Max Pending request time (ms)")
	flag.Int64Var(&config.ProgressThreshold, "progress", 60000, "Threshold for making progress reducing queue (ms)")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug output")
	flag.Parse()
	if config.Debug {
		debug = log.New(log.Writer(), "DEBUG ", log.Ldate|log.Ltime|log.Lshortfile|log.LUTC)
	} else {
		debug = log.New(io.Discard, "DEBUG ", log.Ldate|log.Ltime|log.Lshortfile|log.LUTC)
	}
	rx := regexp.MustCompile(`^(.*?)enc:([[:xdigit:]]+)(.*)$`)
	m := rx.FindStringSubmatch(config.Password)
	if len(m) > 0 {
		newvalue, err := aesdecrypt(m[2])
		if err != nil {
			log.Printf("Unable to decrypt password")
		} else {
			config.Password = newvalue
		}
	}
}

type db_queue_entry struct {
	qsize  uint32
	tstamp int64
}

var db_queue = make(map[string]*db_queue_entry)

func get_queue_entry(dbname string) *db_queue_entry {
	q, ok := db_queue[dbname]
	if !ok {
		debug.Printf("Start DB %s progress tracking", dbname)
		q = &db_queue_entry{
			qsize:  0,
			tstamp: time.Now().UnixMilli(),
		}
		db_queue[dbname] = q
	}
	return q
}

func reset_db_queue(dbname string) {
	q := get_queue_entry(dbname)
	q.qsize = 0
	q.tstamp = time.Now().UnixMilli()
}

func track_db_queue(dbname string, current_qsize uint32, tnow int64) error {
	q := get_queue_entry(dbname)
	if q.qsize >= current_qsize {
		debug.Printf("DB %s queue down from %d to %d", dbname, q.qsize, current_qsize)
		q.qsize = current_qsize
		q.tstamp = tnow
		return nil
	}
	debug.Printf("DB %s queue up from %d to %d", dbname, q.qsize, current_qsize)
	q.qsize = current_qsize
	if q.tstamp-tnow < config.ProgressThreshold {
		return nil
	}
	log.Printf("DB %s queue not making progress since %d, now at %d", dbname, q.tstamp, q.qsize)
	return fmt.Errorf("Database is not making progress since %d", q.tstamp)
}

func ping_db(client immuclient.ImmuClient, ctx context.Context, dbname string) error {
	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: dbname})
	if err != nil {
		debug.Printf("Failed to use database %s. Reason: %s", dbname, err.Error())
		return err
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	health, err := client.Health(ctx)
	if err != nil {
		debug.Printf("Failed to get database %s health. Reason: %s", dbname, err.Error())
		return err
	}
	if health.PendingRequests == 0 {
		debug.Printf("Database %s has 0 pending request. [last tx: %s]", dbname, time.UnixMilli(health.LastRequestCompletedAt))
		reset_db_queue(dbname)
		_, err = client.CurrentState(ctx)
		if err != nil {
			debug.Printf("Failed to get database %s state. Reason: %s", dbname, err.Error())
			return err
		}
		debug.Printf("Database %s ok", dbname)
		return nil
	}
	tnow := time.Now().UnixMilli()
	elapsed := tnow - health.LastRequestCompletedAt
	debug.Printf("DB %s elapsed %d [last tx: %s]", dbname, elapsed, time.UnixMilli(health.LastRequestCompletedAt))
	if elapsed > config.MaxReqWait {
		return fmt.Errorf("Database unresponsive since %d", health.LastRequestCompletedAt)
	}
	if err = track_db_queue(dbname, health.PendingRequests, tnow); err != nil {
		return err
	}
	debug.Printf("Database %s ok", dbname)
	return nil
}

type rettype struct {
	ret interface{}
	err error
}

func timed_database_list(client immuclient.ImmuClient, ctx context.Context) ([]string, error) {
	var databases []string
	c := make(chan rettype, 1)
	go func() {
		dbs, err := client.DatabaseList(ctx)
		c <- rettype{ret: dbs, err: err}
	}()
	select {
	case t := <-c:
		if t.err != nil {
			debug.Printf("Failed to get database list. Reason: %s", t.err.Error())
			return databases, t.err
		}
		dbs := t.ret.(*schema.DatabaseListResponse)
		for _, s := range dbs.Databases {
			databases = append(databases, s.DatabaseName)
		}
	case <-time.After(time.Duration(config.Timeout) * time.Second):
		debug.Printf("Timeout fetching database list")
		return databases, fmt.Errorf("DatabaseList timeout")
	}
	return databases, nil
}

func timed_login(client immuclient.ImmuClient, ctx context.Context, username, password []byte) (*schema.LoginResponse, error) {
	c := make(chan rettype, 1)
	go func() {
		login, err := client.Login(ctx, username, password)
		c <- rettype{ret: login, err: err}
	}()
	select {
	case t := <-c:
		return t.ret.(*schema.LoginResponse), t.err
	case <-time.After(time.Duration(config.Timeout) * time.Second):
		return nil, fmt.Errorf("Login timeout")
	}
}

func scan_all() bool {
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)
	client, err := immuclient.NewImmuClient(opts)
	if err != nil {
		log.Printf("Failed to connect. Reason: %s", err.Error())
		return false
	}
	defer func() { _ = client.Disconnect() }()
	ctx := context.Background()
	login, err := timed_login(client, ctx, []byte(config.Username), []byte(config.Password))
	if err != nil {
		log.Printf("Failed to login. Reason: %s", err.Error())
		return false
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))
	defer func() { _ = client.Logout(ctx) }()
	dbs, err := timed_database_list(client, ctx)
	if err != nil {
		log.Printf("Failed to get database list. Reason: %s", err.Error())
		return false
	}
	testingdb := len(dbs)
	for _, db := range dbs {
		c1 := make(chan error, 1)
		go func() { c1 <- ping_db(client, ctx, db) }()
		select {
		case err = <-c1:
			if err == nil {
				testingdb -= 1
			}
		case <-time.After(time.Duration(config.Timeout) * time.Second):
			log.Printf("Database %s failed to respond", db)
		}
	}
	return testingdb == 0
}

func get_status(w http.ResponseWriter, req *http.Request) {
	if immudb_status.get_status() {
		fmt.Fprintf(w, "OK\n")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "FAIL\n")
	}
}

func version(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, `{"Version":"%s-%s", "Buildtime":"%s"}`+"\n", Version, Commit, Buildtime)
}

func livez(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "alive\n")
}

func main() {
	http.HandleFunc("/livez", livez)
	http.HandleFunc("/immustatus", get_status)
	http.HandleFunc("/version", version)
	go log.Printf("%v", http.ListenAndServe(":8085", nil))
	for {
		debug.Printf("Scanning")
		c := make(chan bool, 1)
		go func() { c <- scan_all() }()
		select {
		case ret := <-c:
			if ret {
				debug.Printf("Scanning completed successfully")
				immudb_status.success()
			} else {
				immudb_status.fail()
				debug.Printf("Scanning completed unsuccessfully")
			}
		case <-time.After(300 * time.Second):
			debug.Printf("Scanning failed [timeout]")
			immudb_status.fail()
			<-c
		}
		debug.Printf("Sleeping")
		time.Sleep(time.Duration(config.Delay) * time.Second)
	}
}
