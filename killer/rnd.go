package main

import (
	"math/rand"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var rndChan chan []byte

func randStringBytesMaskImprSrcUnsafe(n int) {
	b := make([]byte, n)
	// A Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	rndChan <- b
}

func getRnd() []byte {
	ret := <-rndChan
	return ret
}

func startRnd(size int) {
	if config.Seed == 0 {
		rand.Seed(time.Now().UnixNano())
	}
	rndChan = make(chan []byte, 65536)
	go func() {
		for {
			randStringBytesMaskImprSrcUnsafe(size)
		}
	}()
}
