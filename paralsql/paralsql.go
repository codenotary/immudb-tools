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
	IpAddr     string
	Port       int
	Username   string
	Password   string
	DBName     string
	Debug      bool
	TxSize     int
	InsertSize int
	Workers    int
	Rows       int
	Duration   int
	Autoincr   bool
	SecIndex   bool
	MaxTries   int
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
	flag.BoolVar(&c.SecIndex, "sec-index", false, "add secondary index")
	flag.IntVar(&c.TxSize, "txsize", 256, "Transaction size")
	flag.IntVar(&c.InsertSize, "insert-size", 1, "Insert size (values per insert)")
	flag.IntVar(&c.Rows, "rows", 1000, "Rows to be inserted by a worker")
	flag.IntVar(&c.Duration, "duration", 0, "Total test duration in seconds(overrides rows)")
	flag.IntVar(&c.MaxTries, "max-tries", 25, "Max number of attempt to replay a failed transaction")
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
	tx := MakeTx(ctx, client, "init", 100, c.MaxTries)
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
	if c.SecIndex {
		q = q + "CREATE INDEX ON logs(ts); CREATE INDEX ON logs(severity)"
	}
	tx.Add(q)
	tx.Commit()
	client.CloseSession(ctx)
}

func worker(c *cfg, wid int) int {
	debug.Printf("Starting worker %d", wid)
	client, ctx := connect(c)
	tx := MakeTx(ctx, client, fmt.Sprintf("W%2.2d", wid), c.TxSize, c.MaxTries)
	qt0 := `insert into logs(id, ts, address, severity, facility, log) values `
	qt1 := `insert into logs(ts, address, severity, facility, log) values `
	v0 := `( %d, NOW(), '%s', %d, %d, '%s')`
	v1 := `(NOW(), '%s', %d, %d, '%s')`

	var i int
	for i = 0; runForever.Load() || i < c.Rows; i++ {
		var q string
		if c.Autoincr {
			q = qt1
		} else {
			q = qt0
		}

		for j := 0; j < c.InsertSize; j++ {
			ip := <-randIP
			sev := <-randByte
			fac := <-randByte
			log := <-randLog
			msg := fmt.Sprintf("W%2.2d:%d-%s", wid, i, log)
			var qvar string
			if c.Autoincr {
				qvar = fmt.Sprintf(v1, ip, sev, fac, msg)
			} else {
				id := <-seqId
				qvar = fmt.Sprintf(v0, id, ip, sev, fac, msg)
			}
			if j > 0 {
				q = q + " , "
			}
			q = q + qvar
		}
		debug.Printf("QUERY: %s", q)
		tx.Add(q)
	}
	tx.Commit()
	client.CloseSession(ctx)
	return i * c.InsertSize
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
	log.Printf("Starting %d workers, txsize %d, insert size %d\n", c.Workers, c.TxSize, c.InsertSize)
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
