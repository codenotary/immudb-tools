package main

import (
	"math/rand"
	"fmt"
)

var randIP chan string
var randByte chan byte
var randLog chan string

func genIP() {
	for {
		t := rand.Int63()
		randIP <- fmt.Sprintf("%d.%d.%d.%d",t&255, (t>>8)&255, (t>>16)&255, (t>>24)&255)
	}
}

func genByte() {
	for {
		t := rand.Int63()
		randByte <- byte(t&255)
		randByte <- byte((t>>8)&255)
		randByte <- byte((t>>16)&255)
		randByte <- byte((t>>24)&255)
		randByte <- byte((t>>32)&255)
		randByte <- byte((t>>40)&255)
		randByte <- byte((t>>48)&255)
		randByte <- byte((t>>54)&255)
	}
}

func genLog() {
	for {
		t := rand.Int63()
		randLog <- fmt.Sprintf("%X",t)
	}
}

func init() {
	randIP = make(chan string, 256)
	randByte = make(chan byte, 256)
	randLog = make(chan string, 256)
	go genIP()
	go genByte()
	go genLog()
}
