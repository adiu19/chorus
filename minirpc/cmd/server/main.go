package main

import (
	"log"

	"github.com/chorus/minirpc"
)

func main() {
	s := minirpc.NewServer()
	s.Handle("Stream", minirpc.StreamHandler())

	log.Fatal(s.Serve(":9010"))
}
