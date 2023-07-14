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
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"syscall"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/replication"
	"google.golang.org/grpc/metadata"
)

var (
	Version   = "0.0"
	Buildtime = "00"
	Commit    = "00"
	AESKey    = "NOKEY"
	signals   = make(chan os.Signal, 1)
)

var config struct {
	MasterAddr                   string
	MasterPort                   int
	MasterUsername               string
	MasterPassword               string
	ReplicaAddr                  string
	ReplicaPort                  int
	ReplicaUsername              string
	ReplicaPassword              string
	FollowerUsername             string
	FollowerPassword             string
	Port                         int
	Delay                        int
	Datadir                      string
	ReplicationSync              string
	PrefetchTxBufferSize         int
	ReplicationCommitConcurrency int
	AllowTxDiscarding            bool
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if envAddr, ok := os.LookupEnv("MASTER_ADDRESS"); ok {
		config.MasterAddr = envAddr
	}
	if envAddr, ok := os.LookupEnv("REPLICA_ADDRESS"); ok {
		config.ReplicaAddr = envAddr
	}
	config.MasterPassword = "immudb"
	if envPw, ok := os.LookupEnv("MASTER_PASSWORD"); ok {
		config.MasterPassword = envPw
	}
	config.ReplicaPassword = "immudb"
	if envPw, ok := os.LookupEnv("REPLICA_PASSWORD"); ok {
		config.ReplicaPassword = envPw
	}
	config.FollowerPassword = "zyQQurat.0"
	if envPw, ok := os.LookupEnv("FOLLOWER_PASSWORD"); ok {
		config.FollowerPassword = envPw
	}
	config.Datadir = "/var/lib/immudb"
	if envDd, ok := os.LookupEnv("DATADIR"); ok {
		config.Datadir = envDd
	}
	flag.StringVar(&config.MasterAddr, "master-addr", config.MasterAddr, "IP address of immudb master [MASTER_ADDRESS]")
	flag.IntVar(&config.MasterPort, "master-port", 3322, "Port number of immudb master")
	flag.StringVar(&config.MasterUsername, "master-user", "immudb", "Admin username for master immudb")
	flag.StringVar(&config.MasterPassword, "master-pass", config.MasterPassword, "Admin password for master immudb [MASTER_PASSWORD]")

	flag.StringVar(&config.ReplicaAddr, "replica-addr", config.ReplicaAddr, "IP address of immudb replica [REPLICA_ADDRESS]")
	flag.IntVar(&config.ReplicaPort, "replica-port", 3322, "Port number of immudb replica")
	flag.StringVar(&config.ReplicaUsername, "replica-user", "immudb", "Admin username for replica immudb")
	flag.StringVar(&config.ReplicaPassword, "replica-pass", config.ReplicaPassword, "Admin password for replica immudb [REPLICA_PASSWORD]")

	flag.StringVar(&config.FollowerUsername, "follower-user", "follower", "Follower username for immudb databases")
	flag.StringVar(&config.FollowerPassword, "follower-pass", config.FollowerPassword, "Follower password for immudb databases [FOLLOWER_PASSWORD]")

	flag.IntVar(&config.Delay, "delay", 3600, "Delay between compactions in seconds")
	flag.StringVar(&config.Datadir, "data-dir", config.Datadir, "Immudb data directory (for backup) [DATADIR]")

	flag.StringVar(&config.ReplicationSync, "replication-sync", "auto", "Option to sync asynchronous/synchronous replicated databases")
	flag.IntVar(&config.PrefetchTxBufferSize, "replication-prefetch-tx-buffer-size", replication.DefaultPrefetchTxBufferSize, "Maximum number of prefeched transactions")
	flag.IntVar(&config.ReplicationCommitConcurrency, "replication-commit-concurrency", replication.DefaultReplicationCommitConcurrency, "Number of concurrent replications")
	flag.BoolVar(&config.AllowTxDiscarding, "replication-allow-tx-discarding", replication.DefaultAllowTxDiscarding, "Allow precommitted transactions to be discarded if the follower diverges from the master")
	flag.Parse()

	if s, err := aesdecrypt(config.MasterPassword); err == nil {
		config.MasterPassword = s
	}
	if s, err := aesdecrypt(config.ReplicaPassword); err == nil {
		config.ReplicaPassword = s
	}
	if s, err := aesdecrypt(config.FollowerPassword); err == nil {
		config.FollowerPassword = s
	}

	// Signal
	signal.Notify(signals, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
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

func sleep(t0 int64) {
	ti := time.Now().Unix()
	dt := ti - t0
	step := int64(config.Delay)
	nextWake := t0 + ((dt/step)+1)*step
	sleepTime := nextWake - ti
	log.Printf("Sleeping %d", sleepTime)
	time.Sleep(time.Duration(sleepTime) * time.Second)
}

func connect(ctx context.Context, addr string, port int, username, password string) (immuclient.ImmuClient, error) {
	opts := immuclient.DefaultOptions().WithAddress(addr).WithPort(port)

	client := immuclient.NewClient()
	client.Options = opts

	if err := client.OpenSession(ctx, []byte(username), []byte(password), addr); err != nil {
		return client, fmt.Errorf("unable to open session, error %w", err)
	}

	return client, nil
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

const (
	OP_ADD = iota + 1
	OP_DEL
)

type replOperation struct {
	op     int
	src_db string
	dst_db string
}

func processDbList(master_list, replica_list []string) []replOperation {
	var oplist []replOperation
	sort.Strings(master_list)
	sort.Strings(replica_list)
	master_has_defaultdb := false
	replica_has_defaultdb_bak := false
	for i, k := range master_list {
		if k == "defaultdb" {
			master_list = append(master_list[:i], master_list[i+1:]...)
			master_has_defaultdb = true
			break
		}
	}
	for i, k := range replica_list {
		if k == "defaultdb" {
			replica_list = append(replica_list[:i], replica_list[i+1:]...)
			break
		}
	}
	for i, k := range replica_list {
		if k == "defaultdbbak" {
			replica_list = append(replica_list[:i], replica_list[i+1:]...)
			replica_has_defaultdb_bak = true
			break
		}
	}
	if master_has_defaultdb && !replica_has_defaultdb_bak {
		oplist = append(oplist, replOperation{
			op:     OP_ADD,
			src_db: "defaultdb",
			dst_db: "defaultdbbak",
		})
	}
	log.Printf("Master: %v", master_list)
	log.Printf("Replica: %v", replica_list)
	mi, ri := 0, 0
	for mi < len(master_list) && ri < len(replica_list) {
		if master_list[mi] == replica_list[ri] {
			mi += 1
			ri += 1
		} else if master_list[mi] < replica_list[ri] {
			log.Printf("Missing %s, need replication", master_list[mi])
			oplist = append(oplist, replOperation{
				op:     OP_ADD,
				src_db: master_list[mi],
				dst_db: master_list[mi],
			})
			mi += 1
		} else {
			log.Printf("Deleted %s, stop replication", replica_list[ri])
			oplist = append(oplist, replOperation{
				op:     OP_DEL,
				src_db: replica_list[ri],
				dst_db: replica_list[ri],
			})
			ri += 1
		}
	}
	for mi < len(master_list) {
		log.Printf("Missing %s, need replication", master_list[mi])
		oplist = append(oplist, replOperation{
			op:     OP_ADD,
			src_db: master_list[mi],
			dst_db: master_list[mi],
		})
		mi += 1
	}
	for ri < len(replica_list) {
		log.Printf("Deleted %s, stop replication", replica_list[ri])
		oplist = append(oplist, replOperation{
			op:     OP_DEL,
			src_db: replica_list[ri],
			dst_db: replica_list[ri],
		})
		ri += 1
	}
	return oplist
}

func checkUserExists(ctx context.Context, m_cli immuclient.ImmuClient, user string) bool {
	u, err := m_cli.ListUsers(ctx)
	if err != nil {
		log.Printf("Failed to get user list. Reason: %s", err.Error())
		return false
	}

	for _, u := range u.Users {
		if string(u.User) == user {
			return true
		}
	}
	return false
}

func configReplica(m_ctx context.Context, m_cli immuclient.ImmuClient, r_ctx context.Context, r_cli immuclient.ImmuClient, src_db, dst_db string) error {
	log.Printf("CONFIG_REPLICA: %s:%s -> %s:%s", config.MasterAddr, src_db, config.ReplicaAddr, dst_db)
	log.Printf("Fetching settings from master database %s", src_db)
	udr, err := m_cli.UseDatabase(m_ctx, &schema.Database{DatabaseName: src_db})
	if err != nil {
		log.Printf("Failed to use the database. Reason: %s", err.Error())
		return err
	}
	ctx := metadata.NewOutgoingContext(m_ctx, metadata.Pairs("authorization", udr.GetToken()))
	settings_resp, err := m_cli.GetDatabaseSettingsV2(ctx)
	if err != nil {
		log.Printf("Can't fetch setting from master database %s: %s", src_db, err.Error())
		return err
	}
	if checkUserExists(ctx, m_cli, config.FollowerUsername) {
		log.Printf("User already exists on master database %s", src_db)
		err = m_cli.ChangePermission(ctx, schema.PermissionAction_GRANT, config.FollowerUsername, src_db, auth.PermissionAdmin)
		if err != nil {
			log.Printf("Can't update user %s on master database %s: %s", config.FollowerUsername, src_db, err.Error())
			return err
		}
	} else {
		log.Printf("Creating user on master database %s", src_db)
		err = m_cli.CreateUser(m_ctx, []byte(config.FollowerUsername), []byte(config.FollowerPassword), auth.PermissionAdmin, src_db)
		if err != nil {
			log.Printf("Can't create user %s on master database %s: %s", config.FollowerUsername, src_db, err.Error())
			return err
		}
	}
	settings := settings_resp.Settings

	synAcks := settings_resp.Settings.ReplicationSettings.SyncAcks.GetValue()
	isSynchronousDatabase := synAcks > 0
	replication_settings := schema.ReplicationNullableSettings{
		Replica:                      &schema.NullableBool{Value: true},
		MasterDatabase:               &schema.NullableString{Value: src_db},
		MasterAddress:                &schema.NullableString{Value: config.MasterAddr},
		MasterPort:                   &schema.NullableUint32{Value: uint32(config.MasterPort)},
		FollowerUsername:             &schema.NullableString{Value: config.FollowerUsername},
		FollowerPassword:             &schema.NullableString{Value: config.FollowerPassword},
		PrefetchTxBufferSize:         &schema.NullableUint32{Value: uint32(config.PrefetchTxBufferSize)},
		ReplicationCommitConcurrency: &schema.NullableUint32{Value: uint32(config.ReplicationCommitConcurrency)},
	}

	switch config.ReplicationSync {
	case "async": // asynchronous database settings
		if isSynchronousDatabase { // verifying that the primary database is not setup for synchronous replication
			log.Printf("warning: %s not an asynchronous database\n", src_db)
		}
	case "sync": // synchronous database settings
		if !isSynchronousDatabase { // SyncAcks > 0 on the primary database implies that synchronous replication enabled for this db
			return errors.New("not a synchronous database")
		}
		replication_settings.SyncReplication = &schema.NullableBool{Value: true}
		replication_settings.AllowTxDiscarding = &schema.NullableBool{Value: config.AllowTxDiscarding}
	case "auto": // sync database with any mode
		if synAcks > 0 {
			replication_settings.SyncReplication = &schema.NullableBool{Value: true}
			replication_settings.AllowTxDiscarding = &schema.NullableBool{Value: config.AllowTxDiscarding}
		}
	}

	settings.ReplicationSettings = &replication_settings

	log.Printf("Creating database %s on replica server", dst_db)
	_, err = r_cli.CreateDatabaseV2(r_ctx, dst_db, settings)
	if err != nil {
		log.Printf("Error creating replica database %s: %s", dst_db, err.Error())
		return err
	}
	return nil
}

func analyze_db(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, time.Second)
	masterClient, err := connect(tctx, config.MasterAddr, config.MasterPort, config.MasterUsername, config.MasterPassword)
	if err != nil {
		cancel()
		return fmt.Errorf("unable to connect to Master Addr %s Port %d error %w", config.MasterAddr, config.Port, err)
	}

	defer func() { _ = masterClient.CloseSession(ctx) }()

	replicaClient, err := connect(tctx, config.ReplicaAddr, config.ReplicaPort, config.ReplicaUsername, config.ReplicaPassword)
	if err != nil {
		cancel()
		return fmt.Errorf("unable to connect to Replica Addr %s Port %d error %w", config.ReplicaAddr, config.ReplicaPort, err)
	}
	defer func() { _ = replicaClient.CloseSession(ctx) }()
	cancel()

	masterDBs := db_list(ctx, masterClient)
	replicaDBs := db_list(ctx, replicaClient)
	ops := processDbList(masterDBs, replicaDBs)
	for _, o := range ops {
		log.Printf("- %v", o)
		switch o.op {
		case OP_ADD:
			_ = configReplica(ctx, masterClient, ctx, replicaClient, o.src_db, o.dst_db)
		case OP_DEL:
			log.Printf("Ignoring orphaned database %s", o.dst_db)
		}
	}

	return nil
}

func replicatorLoop(ctx context.Context) {
	t := time.NewTicker(time.Second * 60)

	for {
		select {
		case <-t.C:
			if BackupInfo.is_running() {
				continue
			}
			if err := analyze_db(ctx); err != nil {
				log.Printf("unexpected error analyzing DBs %v", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	log.Printf("Replicator %s [%s] @ %s", Version, Commit, Buildtime)
	ctx, cancel := context.WithCancel(context.Background())
	go rest_interface()
	go replicatorLoop(ctx)
	<-signals
	cancel()
}
