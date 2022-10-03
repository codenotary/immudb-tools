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
	"flag"
	"io/ioutil"
	"log"
	"time"

	"gopkg.in/yaml.v2"
)

type (
	Conf struct {
		Master    []server `yaml:"master"`
		Followers []server `yaml:"followers"`
	}

	server struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
		Name string `yaml:"name"`
		DB   string `yaml:"db"`
	}
)

func (s *Conf) init(path string) {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, s)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
}

func main() {
	var (
		interval = flag.Duration("interval", 1*time.Second, "Allows you to specify the interval between output updates.")
		path     = flag.String("path", "conf.yaml", "Allows you to specify the configuration file for the watcher.")
	)
	flag.Parse()

	var c Conf
	c.init(*path)

	clients := make([]*StateClient, 0, 1)
	var id int
	for _, srv := range c.Master {
		id += 1
		client, err := NewStateClient(id, srv.Name, srv.Host, srv.Port, srv.DB)
		if err != nil {
			log.Fatalf("client err #%v ", err)
		}
		clients = append(clients, client)
	}

	for _, srv := range c.Followers {
		id += 1
		client, err := NewStateClient(id, srv.Name, srv.Host, srv.Port, srv.DB)
		if err != nil {
			log.Fatalf("client err #%v ", err)
		}
		clients = append(clients, client)
	}

	watcher := NewWatcher(clients)
	Run(watcher, *interval)
}
