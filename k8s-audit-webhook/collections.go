/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type collField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type collIndex struct {
	Fields   []string `json:"fields"`
	IsUnique bool     `json:"isUnique"`
}
type collection struct {
	IdFieldName string      `json:"idFieldName"`
	Fields      []collField `json:"fields"`
	Indexes     []collIndex `json:"indexes"`
}

func createCollection() {
	log.Printf("Creating collection %s", config.collection)
	coll := collection{
		Fields: []collField{
			{"auditID", "STRING"},
			{"stage", "STRING"},
			{"level", "STRING"},
			{"requestReceivedTimestamp", "STRING"},
			{"userAgent", "STRING"},
			{"verb", "STRING"},
			{"objectRef.namespace", "STRING"},
			{"objectRef.resource", "STRING"},
			{"objectRef.name", "STRING"},
		},
		Indexes: []collIndex{
			// {Fields: []string{"auditID", "stage"}, IsUnique: true},
			{Fields: []string{"auditID"}, IsUnique: false},
			{Fields: []string{"stage"}, IsUnique: false},
			{Fields: []string{"level"}, IsUnique: false},
			{Fields: []string{"objectRef.namespace"}, IsUnique: false},
			{Fields: []string{"objectRef.resource"}, IsUnique: false},
			{Fields: []string{"objectRef.name"}, IsUnique: false},
		},
	}
	url := fmt.Sprintf("%s/ics/api/v1/ledger/%s/collection/%s", config.url, config.ledger, config.collection)
	jsonData, err := json.Marshal(coll)
	if err != nil {
		log.Fatalf("Error marshaling payload: %s", err.Error())
		return
	}
	if config.dump {
		dump(jsonData)
	}

	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("X-API-Key", config.apikey)
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Fatalf("Error sending request: %s", err.Error())
		return
	}
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		log.Printf("Collection Created")
		return
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("client: could not read response body: %s\n", err)
	}
	log.Fatalf("Response body: %s\n", resBody)
}

func checkCollection() {
	url := fmt.Sprintf("%s/ics/api/v1/ledger/%s/collection/%s", config.url, config.ledger, config.collection)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Error creating http request: %s", err.Error())
		return
	}
	request.Header.Set("Content-Type", "application/json; charset=UTF-8")
	request.Header.Set("X-API-Key", config.apikey)
	res, err := http.DefaultClient.Do(request)
	if err != nil {
		log.Printf("Error sending request: %s", err.Error())
		return
	}
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		log.Printf("Collection OK")
		return
	}
	if res.StatusCode == 404 {
		createCollection()
		return
	}
	resBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("client: could not read response body: %s\n", err)
	}
	log.Fatalf("Response body: %s\n", resBody)

}
