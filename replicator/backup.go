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
	"archive/tar"
	"context"
	"errors"
	"github.com/codenotary/immudb/pkg/api/schema"
	gzip "github.com/klauspost/pgzip" // "compress/gzip"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrBackupInProgress = errors.New("A backup operation is already in progress")
	ErrUnkownTarget     = errors.New("Unkown backup target")
)

type backupInfo struct {
	mx          sync.Mutex
	progress    int
	num_db      int
	bytes_total uint64
	bytes_read  uint64
}

var BackupInfo = &backupInfo{
	progress:    0,
	num_db:      0,
	bytes_total: 0,
	bytes_read:  0,
}

func (bu *backupInfo) start() error {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	if bu.progress != 0 {
		return ErrBackupInProgress
	}
	bu.progress = 1
	bu.bytes_total = 0
	bu.bytes_read = 0
	return nil
}

func (bu *backupInfo) end() {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	bu.progress = 0
	bu.bytes_read = bu.bytes_total
}

func (bu *backupInfo) is_running() bool {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	return bu.progress != 0
}

func (bu *backupInfo) set_n_db(n_db int) {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	bu.num_db = n_db
}

func (bu *backupInfo) inc_progress() {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	bu.progress += 1
}

func (bu *backupInfo) set_total_bytes(n uint64) {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	bu.bytes_total = n
}

func (bu *backupInfo) add_bytes(n uint64) {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	bu.bytes_read += n
}

func (bu *backupInfo) stat() (int, float64) {
	bu.mx.Lock()
	defer bu.mx.Unlock()
	p1 := 0
	if bu.num_db != 0 {
		p1 = bu.progress * 100 / bu.num_db
	}
	p2 := 0.0
	if bu.bytes_read != 0 {
		p2 = float64(bu.bytes_read) / float64(bu.bytes_total)
	}
	return p1, p2
}

func doBackup(req backupRequest) {
	if err := BackupInfo.start(); err != nil {
		log.Printf("Error starting backup: %s", err.Error())
		return
	}
	defer BackupInfo.end()
	BackupInfo.set_total_bytes(calcDirSize(config.Datadir))
	log.Printf("Backup started")
	ctx, cli := connect(config.ReplicaAddr, config.ReplicaPort, config.ReplicaUsername, config.ReplicaPassword)
	if cli == nil {
		return
	}
	defer func() { _ = cli.Disconnect() }()
	dbs := db_list(ctx, cli)
	BackupInfo.set_n_db(len(dbs))
	out, err := createBackupTarget(req)
	if err != nil {
		log.Printf("Error creating backup writer: %s", err.Error())
		return
	}
	defer out.Close()
	gw := gzip.NewWriter(out)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	cpBuffer := make([]byte, 1048576)
	for _, db := range dbs {
		if db == "defaultdb" {
			continue
		}
		log.Printf("Backing up %s", db)
		_, err = cli.UnloadDatabase(ctx, &schema.UnloadDatabaseRequest{Database: db})
		if err != nil {
			log.Printf("Error unloading database: %s", err.Error())
			continue
		}
		if err = backupPath(ctx, tw, path.Join(config.Datadir, db), config.Datadir, cpBuffer); err != nil {
			log.Printf("Error backing up database: %s", err.Error())
		}
		_, err = cli.LoadDatabase(ctx, &schema.LoadDatabaseRequest{Database: db})
		if err != nil {
			log.Printf("Error loading database: %s", err.Error())
			continue
		}
		BackupInfo.inc_progress()
	}
}

func createBackupTarget(req backupRequest) (io.WriteCloser, error) {
	switch req.TargetType {
	case "file":
		return os.Create(req.TargetPath)
	case "sftp":
		return sftp_create(req)
	case "s3":
		return s3_create(req)
	}

	return nil, ErrUnkownTarget
}

func calcDirSize(pat string) uint64 {
	var total uint64 = 0
	err := filepath.Walk(pat, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total = total + uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		log.Printf("Error calculating size: %s", err.Error())
	}
	return total
}

func backupPath(ctx context.Context, tw *tar.Writer, src string, rootpath string, cpBuffer []byte) error {
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if err = addToArchive(tw, path, rootpath, cpBuffer); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Error backing up files from %s", src)
	}
	return err
}

type readCounter struct {
	io.Reader
	info *backupInfo
}

func (rc *readCounter) Read(p []byte) (int, error) {
	n, err := rc.Reader.Read(p)
	if rc.info != nil {
		rc.info.add_bytes(uint64(n))
	}
	return n, err
}

func addToArchive(tw *tar.Writer, filename string, stripdir string, cpBuffer []byte) error {
	// Open the file which will be written into the archive
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	rcfile := &readCounter{Reader: file, info: BackupInfo}
	// Get FileInfo about our file providing file size, mode, etc.
	info, err := file.Stat()
	if err != nil {
		return err
	}

	// Create a tar Header from the FileInfo data
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return err
	}

	// Use full path as name (FileInfoHeader only takes the basename)
	// If we don't do this the directory strucuture would
	// not be preserved
	// https://golang.org/src/archive/tar/common.go?#L626
	if stripdir != "" && strings.HasPrefix(filename, stripdir) {
		header.Name = path.Join(".", filename[len(stripdir):])
	} else {
		header.Name = filename
	}

	// Write file header to the tar archive
	err = tw.WriteHeader(header)
	if err != nil {
		return err
	}

	// Copy file content to tar archive
	if cpBuffer != nil {
		_, err = io.CopyBuffer(tw, rcfile, cpBuffer) // file)
	} else {
		_, err = io.Copy(tw, rcfile) // file)
	}
	if err != nil {
		return err
	}
	return nil
}
