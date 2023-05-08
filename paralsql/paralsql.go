package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	immudb "github.com/codenotary/immudb/pkg/client"
)

type cfg struct {
	IpAddr   string
	Port     int
	Username string
	Password string
	DBName   string
	Debug    bool
	TxSize   int
	Workers  int
	Rows     int
}

func parseConfig() (c *cfg) {
	c = &cfg{}
	flag.StringVar(&c.IpAddr, "addr", "", "IP address of immudb server")
	flag.IntVar(&c.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&c.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&c.Password, "pass", "immudb", "Password for authenticating to immudb")
	flag.StringVar(&c.DBName, "db", "defaultdb", "Name of the database to use")
	flag.IntVar(&c.Workers, "workers", 1, "Number of workers")
	flag.BoolVar(&c.Debug, "debug", false, "log level: debug")
	flag.IntVar(&c.TxSize, "txsize", 256, "Transaction size")
	flag.IntVar(&c.Rows, "rows", 1000, "Rows to be inserted by a worker")
	flag.Parse()
	return
}

var debug *log.Logger

func init_log(c *cfg) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if !c.Debug {
		debug = log.New(io.Discard, "DEBUG", 0)
	} else {
		debug = log.New(os.Stderr, "DEBUG", log.LstdFlags|log.Lshortfile)
	}
}

func connect(config *cfg) (immudb.ImmuClient, context.Context) {
	log.Print("Connecting")
	ctx := context.Background()
	opts := immudb.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	var client immudb.ImmuClient

	client = immudb.NewClient().WithOptions(opts)

	err := client.OpenSession(ctx, []byte(config.Username), []byte(config.Password), config.DBName)
	if err != nil {
		log.Fatalln("Failed to connect. Reason:", err)
	}
	return client, ctx
}

func createTable(c *cfg) {
	client, ctx := connect(c)
	tx := MakeTx(ctx, client, "init", 100)
	q := `CREATE TABLE IF NOT EXISTS logs (
		id INTEGER AUTO_INCREMENT,
		ts TIMESTAMP,
		address VARCHAR NOT NULL,
		severity INTEGER,
		facility INTEGER,
		log VARCHAR,
		PRIMARY KEY(id)
	);`
	tx.Add(q)
	tx.Commit()
	client.CloseSession(ctx)
}

func worker(c *cfg, wid int) {
	log.Printf("Starting worker %d", wid)
	client, ctx := connect(c)
	tx := MakeTx(ctx, client, fmt.Sprintf("W%2.2d", wid), c.TxSize)
	qt := `insert into logs(ts, address, severity, facility, log) values ( NOW(), '%s', %d, %d, '%s');`
	for i := 0; i < c.Rows; i++ {
		ip := <-randIP
		sev := <-randByte
		fac := <-randByte
		log := <-randLog
		msg := fmt.Sprintf("W%2.2d:%d-%s", wid, i, log)
		q := fmt.Sprintf(qt, ip, sev, fac, msg)
		tx.Add(q)
	}
	tx.Commit()
	client.CloseSession(ctx)
}

func main() {
	c := parseConfig()
	init_log(c)
	createTable(c)
	end := make(chan bool)
	t0 := time.Now()
	for i := 0; i < c.Workers; i++ {
		go func(i int) {
			worker(c, i)
			end <- true
		}(i)
	}
	for i := 0; i < c.Workers; i++ {
		<-end
	}
	log.Printf("Elapsed: %.3f", time.Since(t0).Seconds())
	log.Printf("Failed (and retried) TX: %d", retries)
}
