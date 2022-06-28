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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func rest_interface() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/backup/start", startBackup)
	http.HandleFunc("/backup/status", statusBackup)
	err := http.ListenAndServe(":8081", nil)
	log.Printf("Terminating rest interface [%s]", err.Error())
}

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "replicator v%s-%s @%s\n", Version, Commit, Buildtime)
}

type backupRequest struct {
	TargetType   string `json:"type"`
	TargetPath   string `json:"path"` // for s3, use s3://bucket/key
	TargetHost   string `json:"host"` // also used for s3 region
	TargetPort   int    `json:"port"`
	AuthUsername string `json:"username"`
	AuthPassword string `json:"password"`
}

func statusBackup(w http.ResponseWriter, r *http.Request) {
	p1, p2 := BackupInfo.stat()
	fmt.Fprintf(w, "{\"running\": %t, \"progress\": %d, \"progress_bytes\": %f}\n", BackupInfo.is_running(), p1, p2)
}

func startBackup(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p1, p2 := BackupInfo.stat()
		fmt.Fprintf(w, "{\"running\": %t, \"progress\": %d, \"progress_bytes\": %f}\n", BackupInfo.is_running(), p1, p2)
	case http.MethodPost:
		var req backupRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Bad request: "+err.Error(), http.StatusBadRequest)
			break
		}
		if BackupInfo.is_running() {
			http.Error(w, "Bad request: backup already in progress", http.StatusBadRequest)
			break
		}
		go doBackup(req)
		fmt.Fprintf(w, "OK")
	default:
		http.Error(w, "Wrong method", http.StatusBadRequest)
	}
}
