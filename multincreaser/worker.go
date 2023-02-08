package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	immuclient "github.com/codenotary/immudb/pkg/client"
)

func connect(jobid string) (context.Context, immuclient.ImmuClient) {
	tokenfile := "token-" + randID(8)
	log.Printf("Job %s Tokenfile: %s", jobid, tokenfile)
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port).WithTokenFileName(tokenfile)
	client := immuclient.NewClient().WithOptions(opts)
	ctx := context.Background()
	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return ctx, client
}

func expBackoff(f func() (any, error)) (any, error) {
	var err error
	var ret any
	for i := 0; i < 8; i++ {
		ret, err = f()
		if err == nil {
			return ret, err
		}
		time.Sleep(time.Millisecond * (10 << i))
	}
	ret, err = f()
	return ret, err
}

var mx sync.Mutex

func IncrementKey(client immuclient.ImmuClient, ctx context.Context, job, k *string) (any, error) {
	if config.Sync {
		mx.Lock()
		defer mx.Unlock()
	}
	r, err := client.SQLQuery(ctx, "SELECT value FROM t WHERE id=@id", map[string]interface{}{"id": *k}, true)
	if err != nil {
		log.Printf("SELECT error: %s", err.Error())
		return nil, err
	}
	ret := r.Rows[0]
	value := ret.Values[0].GetN()
	value = value + 1
	log.Printf("%s Setting %s to %d", *job, *k, value)
	_, err = client.SQLExec(ctx, "UPDATE t SET value=@val WHERE id=@id",
		map[string]interface{}{"id": *k, "val": value})
	if err != nil {
		log.Printf("%s UPDATING %s to %d error: %s", *job, *k, value, err.Error())
	}
	return nil, err
}

func Worker(n int, total *int64, keys *Keyspace) {
	jobid := fmt.Sprintf("JOB%02d", n)
	ctx, client := connect(jobid)
	defer client.CloseSession(ctx)
	for i := 0; i < config.WorkSize; i++ {
		k := keys.GetRandomKey()
		_, err := expBackoff(func() (any, error) { return IncrementKey(client, ctx, &jobid, k) })
		if err == nil {
			atomic.AddInt64(total, 1)
		}
	}
}

func InitDB(keys *Keyspace) {
	ctx, client := connect("0")
	defer client.CloseSession(ctx)
	_, err := client.SQLExec(ctx, "CREATE TABLE IF NOT EXISTS t (id VARCHAR[256], value INTEGER, PRIMARY KEY id)", nil)
	if err != nil {
		log.Fatal("Creation error: ", err.Error())
	}
	for i := 0; i < config.KeySpace; i++ {
		k := keys.GetKey(i)
		_, err = client.SQLExec(ctx, "UPSERT INTO t(id, value) VALUES(@id, @val)",
			map[string]interface{}{"id": *k, "val": 0})
		if err != nil {
			log.Fatal("Creation error: ", *k, "/", err.Error())
		}
	}
}

func TotalDB(keys *Keyspace) (total int64) {
	ctx, client := connect("0")
	defer client.CloseSession(ctx)
	for i := 0; i < config.KeySpace; i++ {
		k := keys.GetKey(i)
		r, err := client.SQLQuery(ctx, "SELECT value FROM t WHERE id=@id",
			map[string]interface{}{"id": *k}, true)
		if err != nil {
			log.Fatal("Totalling error: ", *k, "/", err.Error())
		}
		val := r.Rows[0].Values[0].GetN()
		total += val
	}
	return
}
