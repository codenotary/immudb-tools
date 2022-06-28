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
	"fmt"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
)

type sftpWriteCloser struct {
	tcp_layer  io.Closer
	ssh_layer  io.Closer
	sftp_layer io.WriteCloser
}

func (s sftpWriteCloser) Write(p []byte) (int, error) {
	return s.sftp_layer.Write(p)
}

func (s sftpWriteCloser) Close() error {
	if err := s.tcp_layer.Close(); err != nil {
		return err
	}
	if err := s.sftp_layer.Close(); err != nil {
		return err
	}
	return s.ssh_layer.Close()
}

func sftp_create(req backupRequest) (io.WriteCloser, error) {
	config := &ssh.ClientConfig{
		User: req.AuthUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(req.AuthPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	conn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", req.TargetHost, req.TargetPort), config)
	if err != nil {
		log.Printf("Unable to connect to %s:%d: %s", req.TargetHost, req.TargetPort, err)
		return nil, err
	}
	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Printf("Unable to create sftp client: %s", err)
		return nil, err
	}
	dstFile, err := client.Create(req.TargetPath)
	if err != nil {
		log.Printf("Unable to create destination file: %s", err)
		return nil, err
	}
	return sftpWriteCloser{
		tcp_layer:  conn,
		ssh_layer:  client,
		sftp_layer: dstFile}, nil
}
