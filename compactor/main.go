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
	"log"
	"os"
	"regexp"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	Version   = "0.0"
	Buildtime = "00"
	Commit    = "00"
	AESKey    = "NOKEY"
)

var config struct {
	Username string
	Password string
	IpAddr   string
	Port     int
	DBName   string
	Delay    int
	Oneshot  bool
	AllDB    bool
	Flush    bool

	CleanupPercentage float64
}

func aesdecrypt(s string) (string, error) {
	rx := regexp.MustCompile(`^(.*?)enc:([[:xdigit:]]+)(.*)$`)
	m := rx.FindStringSubmatch(s)
	if len(m) == 0 {
		return s, nil
	}
	bs, err := hex.DecodeString(m[2])
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

func init() {
	defaultPw := "immudb"
	if envPw, ok := os.LookupEnv("IMMUDB_ADMIN_PASSWORD"); ok {
		defaultPw = envPw
	}
	defaultAddr := "127.0.0.1"
	if envAddr, ok := os.LookupEnv("IMMUDB_ADDRESS"); ok {
		defaultAddr = envAddr
	}
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Compactor %s [%s] @ %s\n\nUsage of %s:\n", Version, Commit, Buildtime, os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEnv variables:\n\t- IMMUDB_ADMIN_PASSWORD\t(optionally encrypted) immudb password\n\t- IMMUDB_ADDRESS\timmudb server address\n")
	}
	flag.StringVar(&config.IpAddr, "addr", defaultAddr, "IP address of immudb server; overrides env variable IMMUDB_ADDRESS")
	flag.IntVar(&config.Port, "port", 3322, "Port number of immudb server")
	flag.StringVar(&config.Username, "user", "immudb", "Username for authenticating to immudb")
	flag.StringVar(&config.Password, "pass", defaultPw, "Password for authenticating to immudb; overrides env variable IMMUDB_ADMIN_PASSWORD")
	flag.IntVar(&config.Delay, "delay", 3600, "Delay between compactions")
	flag.StringVar(&config.DBName, "db", "defaultdb", "Name of the database to use")
	flag.BoolVar(&config.Oneshot, "oneshot", false, "Perform just one compaction then exit")
	flag.BoolVar(&config.AllDB, "all", false, "Compact all databases")
	flag.Float64Var(&config.CleanupPercentage, "cleanup-percentage", -1, "Set cleanup percentage for online compaction (requires oneshot and immudb restart)")
	flag.BoolVar(&config.Flush, "flush", false, "If set, instead of setting cleanup-percentage, does a flush compaction. Note: cleanup-percentage MUST be set")
	flag.Parse()
	var err error
	config.Password, err = aesdecrypt(config.Password)
	if err != nil {
		log.Printf("Unable to decrypt password")
	}

	if config.CleanupPercentage >= 0 && !config.Oneshot {
		log.Fatalf("Setting online compaction percentage requires oneshot flag")
	}
	if config.Flush && config.CleanupPercentage < 0 {
		log.Fatalf("Cleanup-percentage MUST be set when flush is specified")
	}
}

func connect() (context.Context, immuclient.ImmuClient) {
	ctx := context.Background()
	opts := immuclient.DefaultOptions().WithAddress(config.IpAddr).WithPort(config.Port)

	client, err := immuclient.NewImmuClient(opts)
	if err != nil {
		log.Printf("Failed to connect. Reason: %s", err.Error())
		return ctx, nil
	}

	login, err := client.Login(ctx, []byte(config.Username), []byte(config.Password))
	if err != nil {
		log.Printf("Failed to login. Reason: %s", err.Error())
		return ctx, nil
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", login.GetToken()))
	return ctx, client
}

func compact(ctx context.Context, client immuclient.ImmuClient, dbname string) {
	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: dbname})
	if err != nil {
		log.Printf("Failed to use the database. Reason: %s", err.Error())
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	_ = client.CompactIndex(ctx, &emptypb.Empty{})
}

func flush(ctx context.Context, client immuclient.ImmuClient, dbname string) {
	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: dbname})
	if err != nil {
		log.Printf("Failed to use the database. Reason: %s", err.Error())
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))
	_,_ = client.FlushIndex(ctx, float32(config.CleanupPercentage), true)
}

func setOnlineCompaction(ctx context.Context, client immuclient.ImmuClient, dbname string) {
	udr, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: dbname})
	if err != nil {
		log.Printf("Failed to use the database. Reason: %s", err.Error())
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", udr.GetToken()))

	oldSettings, err := client.GetDatabaseSettingsV2(ctx)
	if err != nil {
		log.Printf("Failed to fetch current db settings. Reason: %s", err.Error())
		return
	}

	settings, err := client.UpdateDatabaseV2(ctx, dbname, &schema.DatabaseNullableSettings{
		IndexSettings: &schema.IndexNullableSettings{
			CleanupPercentage: &schema.NullableFloat{Value: float32(config.CleanupPercentage)},
		},
	})
	if err != nil {
		log.Printf("Failed to set cleanup percentage. Reason: %s", err.Error())
		return
	}

	log.Printf(
		"Current cleanup percentage: %f (was previously %f)",
		settings.GetSettings().GetIndexSettings().GetCleanupPercentage().GetValue(),
		oldSettings.GetSettings().GetIndexSettings().GetCleanupPercentage().GetValue(),
	)
}

func db_list(ctx context.Context, client immuclient.ImmuClient) []string {
	var databases []string
	dbs, err := client.DatabaseList(ctx)
	if err != nil {
		log.Printf("Failed to get database list. Reason: %s", err.Error())
	}
	for _, s := range dbs.Databases {
		databases = append(databases, s.DatabaseName)
	}
	return databases
}

func sleep(t0 int64) {
	ti := time.Now().Unix()
	dt := ti - t0
	step := int64(config.Delay)
	nextWake := t0 + ((dt/step)+1)*step
	sleepTime := nextWake - ti
	log.Printf("Sleeping %d", sleepTime)
	time.Sleep(time.Duration(sleepTime) * time.Second)
}

func main() {
	log.Printf("Compactor %s [%s] @ %s", Version, Commit, Buildtime)
	t0 := time.Now().Unix()
	for {
		ctx, client := connect()
		if client == nil {
			panic("Unable to connect")
		}
		var dblist []string
		if config.AllDB {
			dblist = db_list(ctx, client)
		} else {
			dblist = []string{config.DBName}
		}
		for _, db := range dblist {
			if config.Flush {
				log.Printf("Flushing database %s", db)
				flush(ctx, client, db)
			} else if config.CleanupPercentage >= 0 {
				log.Printf("Setting online compaction settings for %s", db)
				setOnlineCompaction(ctx, client, db)
			} else {
				log.Printf("Compacting %s", db)
				compact(ctx, client, db)
			}
		}
		_ = client.Logout(ctx)
		_ = client.Disconnect()
		if config.Oneshot {
			break
		}
		sleep(t0)
	}
	log.Printf("Quitting")
}
