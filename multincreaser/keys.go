package main

import (
	"math/rand"
)

type Keyspace struct {
	keys []string
}

var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-"

func randID(length int) string {
	ll := len(chars)
	b := make([]byte, length)
	rand.Read(b) // generates len(b) random bytes
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%ll]
	}
	return string(b)
}

func NewKeyspace(size int) *Keyspace {
	ks := Keyspace{
		keys: make([]string, size),
	}
	for i := range ks.keys {
		ks.keys[i] = randID(8)
	}
	return &ks
}

func (ks *Keyspace) GetRandomKey() *string {
	n := rand.Int()
	l := len(ks.keys)
	return &ks.keys[n%l]
}

func (ks *Keyspace) GetKey(n int) *string {
	return &ks.keys[n]
}
