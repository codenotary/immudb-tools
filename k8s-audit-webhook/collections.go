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
			{"stageTimestamp", "STRING"},
			{"userAgent", "STRING"},
			{"verb", "STRING"},
		},
		Indexes: []collIndex{
			// {Fields: []string{"auditID", "stage"}, IsUnique: true},
			{Fields: []string{"auditID"}, IsUnique: false},
			{Fields: []string{"stage"}, IsUnique: false},
			{Fields: []string{"level"}, IsUnique: false},
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
