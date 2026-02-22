package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/chorus/minirpc"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: client <method> <payload>\n")
		os.Exit(1)
	}

	method := os.Args[1]
	payload := os.Args[2]

	conn, err := net.Dial("tcp", "localhost:9010")
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send request
	minirpc.WriteFrame(conn, &minirpc.Frame{
		StreamID: 1,
		Type:     minirpc.FrameData,
		Method:   method,
		Payload:  []byte(payload),
	})
	minirpc.WriteFrame(conn, &minirpc.Frame{
		StreamID: 1,
		Type:     minirpc.FrameEndStream,
	})

	// Read responses
	for {
		f, err := minirpc.ReadFrame(conn)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("read: %v", err)
		}
		if f.Type == minirpc.FrameEndStream {
			fmt.Println("\n[stream ended]")
			break
		}
		fmt.Println(string(f.Payload))
	}
}
