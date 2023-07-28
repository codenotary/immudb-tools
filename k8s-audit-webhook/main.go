package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var config struct {
	apikey     string
	url        string
	batchsize  int
	ledger     string
	collection string
	skipTls    bool
	dump       bool
}

type aMsg map[string]any
type auditMsg struct {
	Items []aMsg `json:"items"`
}

type immudbSender struct {
	immuChan chan aMsg
	payload  struct {
		Documents []aMsg `json:"documents"`
	}
}

func (is *immudbSender) audit(w http.ResponseWriter, r *http.Request) {
	var msg auditMsg
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, m := range msg.Items {
		is.immuChan <- m
	}
	log.Printf("Recv %d items", len(msg.Items))
	fmt.Fprintf(w, "ok %d", len(msg.Items))
}

func (is *immudbSender) doWrite() {
	f, _ := os.OpenFile("/tmp/a.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	for _, m := range is.payload.Documents {
		s, _ := json.Marshal(m)
		fmt.Fprintf(f, "%s\n", s)
	}
	is.payload.Documents = []aMsg{}
}

var dumpN = 0

func dump(data []byte) {
	fname := fmt.Sprintf("dump.%d.json", dumpN)
	dumpN=dumpN+1
	f, _ := os.Create(fname)
	defer f.Close()
	f.Write(data)
}

func (is *immudbSender) doSend() {
	log.Printf("Sending request for %d docs", len(is.payload.Documents))
	defer func() {
		is.payload.Documents = []aMsg{}
	}()
	url := fmt.Sprintf("%s/ics/api/v1/ledger/%s/collection/%s/documents", config.url, config.ledger, config.collection)
	jsonData, err := json.Marshal(is.payload)
	if err != nil {
		log.Printf("Error marshaling payload: %s", err.Error())
		return
	}
	if config.dump {
		dump(jsonData)
	}
	request, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
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
	log.Printf("Request send: status code %d", res.StatusCode)
	if res.StatusCode < 200 || res.StatusCode > 299 {
		resBody, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Printf("client: could not read response body: %s\n", err)
		}
		log.Printf("Response body: %s\n", resBody)
	}
}

func (is *immudbSender) senderLoop() {
	is.payload.Documents = []aMsg{}
	d := time.Duration(100 * time.Millisecond)
	t := time.NewTimer(d)
	for {
		select {
		case m := <-is.immuChan:
			is.payload.Documents = append(is.payload.Documents, m)
			if len(is.payload.Documents) == config.batchsize {
				t.Stop()
				is.doSend()
			} else {
				t.Reset(d)
			}
		case <-t.C:
			log.Printf("Timer triggered")
			if len(is.payload.Documents) > 0 {
				log.Printf("Timer writing %d", len(is.payload.Documents))
				is.doSend()
			}
		}
	}
}

func init() {
	flag.StringVar(&config.apikey, "apikey", "", "Immudb Vault API key")
	flag.StringVar(&config.url, "url", "https://vault.immudb.io", "Immudb vault base URL")
	flag.IntVar(&config.batchsize, "batchsize", 100, "Immudb vault submission batch size")
	flag.StringVar(&config.ledger, "ledger", "default", "Immudb vault ledger")
	flag.StringVar(&config.collection, "collection", "auditlog", "Immudb vault collection")
	flag.BoolVar(&config.skipTls, "skiptls", false, "Skip TLS verify")
	flag.BoolVar(&config.dump, "dump", false, "Dumps json requests")
	flag.Parse()
	if config.skipTls {
		log.Printf("Skip tls")
		http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} else {
		log.Printf("Do tls")
	}
}

func main() {
	var sender *immudbSender = &immudbSender{
		immuChan: make(chan aMsg, 32),
	}
	checkCollection()
	go sender.senderLoop()
	http.HandleFunc("/audit", sender.audit)
	err := http.ListenAndServe(":7071", nil)
	log.Printf("Quitting: %s", err.Error())
}
