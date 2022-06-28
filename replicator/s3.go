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
	"errors"
	"github.com/vchain-us/codenotary-cloud/tools/replicator/s3up"
	"io"
	"log"
	"regexp"
)

func s3_create(req backupRequest) (io.WriteCloser, error) {
	m := regexp.MustCompile("^s3://([a-z0-9.-]+)/(.*)$").FindStringSubmatch(req.TargetPath)
	if len(m) == 0 {
		log.Printf("Unable to extract s3 path")
		return nil, errors.New("Unable to extract s3 path")
	}
	bucket := m[1]
	key := m[2]
	region := req.TargetHost
	access_key := req.AuthUsername
	secret_key := req.AuthPassword
	w, err := s3up.Open(key, bucket, region, access_key, secret_key)
	if err != nil {
		log.Printf("Unable to create s3 client: %s", err)
		return nil, err
	}
	return w, nil
}
