package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
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
	Duration int
	Autoincr bool
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
	flag.BoolVar(&c.Autoincr, "autoincrement", false, "use AUTOINCREMENT field")
	flag.IntVar(&c.TxSize, "txsize", 256, "Transaction size")
	flag.IntVar(&c.Rows, "rows", 1000, "Rows to be inserted by a worker")
	flag.IntVar(&c.Duration, "duration", 0, "Total test duration in seconds(overrides rows)")
	flag.Parse()
	return
}

var debug *log.Logger

func init_log(c *cfg) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if !c.Debug {
		debug = log.New(io.Discard, "DEBUG", 0)
	} else {
		debug = log.New(os.Stderr, "DEBUG: ", log.LstdFlags|log.Lshortfile)
	}
}

func connect(config *cfg) (immudb.ImmuClient, context.Context) {
	debug.Print("Connecting")
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
	qb := `CREATE TABLE IF NOT EXISTS logs (
		id INTEGER %s,
		ts TIMESTAMP,
		address VARCHAR NOT NULL,
		severity INTEGER,
		facility INTEGER,
		log VARCHAR,
		PRIMARY KEY(id)
	);`
	var q string
	if c.Autoincr {
		q = fmt.Sprintf(qb, "AUTO_INCREMENT")
	} else {
		q = fmt.Sprintf(qb, "")
	}
	tx.Add(q)
	tx.Commit()
	client.CloseSession(ctx)
}

func worker(c *cfg, wid int) int {
	debug.Printf("Starting worker %d", wid)
	client, ctx := connect(c)
	tx := MakeTx(ctx, client, fmt.Sprintf("W%2.2d", wid), c.TxSize)
	qt0 := `insert into logs(id, ts, address, severity, facility, log) values ( %d, NOW(), '%s', %d, %d, '%s');`
	qt1 := `insert into logs(ts, address, severity, facility, log) values (NOW(), '%s', %d, %d, '%s');`
	var i int
	for i = 0; runForever.Load() || i < c.Rows; i++ {
		ip := <-randIP
		sev := <-randByte
		fac := <-randByte
		log := <-randLog
		msg := fmt.Sprintf("W%2.2d:%d-%s", wid, i, log)
		var q string
		if c.Autoincr {
			q = fmt.Sprintf(qt1, ip, sev, fac, msg)
		} else {
			id := <-seqId
			q = fmt.Sprintf(qt0, id, ip, sev, fac, msg)
		}
		tx.Add(q)
	}
	tx.Commit()
	client.CloseSession(ctx)
	return i
}

var seqId chan int

func genSeq() {
	seqId = make(chan int, 256)

	for s := 0; ; s++ {
		seqId <- s
	}
}

var runForever *atomic.Bool

func main() {
	c := parseConfig()
	init_log(c)
	createTable(c)
	end := make(chan int)
	go genSeq()
	t0 := time.Now()
	runForever = &atomic.Bool{}
	if c.Duration > 0 {
		runForever.Store(true)
		runTimer := time.NewTimer(time.Duration(c.Duration) * time.Second)
		go func() {
			c.Rows = 0
			<-runTimer.C
			runForever.Store(false)
			log.Printf("Quit after running for %d seconds", c.Duration)
		}()
	} else {
		runForever.Store(false)
	}
	log.Printf("Starting %d workers, txsize %d\n", c.Workers, c.TxSize)
	for i := 0; i < c.Workers; i++ {
		go func(i int) {
			end <- worker(c, i)
		}(i)
	}
	total := 0
	for i := 0; i < c.Workers; i++ {
		t := <-end
		total = total + t
	}
	elapsed := time.Since(t0).Seconds()
	log.Printf("Failed (and retried) TX: %d", retries)
	log.Printf("Total Writes: %d, Elapsed: %.3f, %.3f writes/s", total, elapsed, float64(total)/elapsed)
}
