package minirpc

import (
	"log"
	"strings"
)

// StreamHandler provides a handler that sends chunks to a stream reference
func StreamHandler() HandlerFunc {
	return func(stream *Stream) {
		f, err := stream.Recv()
		if err != nil {
			log.Printf("minirpc: Stream recv error: %v", err)
			return
		}

		log.Printf("minirpc: Stream received: %s", string(f.Payload))

		words := strings.Split(string(f.Payload), " ")
		for _, word := range words {
			stream.Send(&Frame{Type: FrameData, Payload: []byte(word)})
		}
		stream.Send(&Frame{Type: FrameEndStream})
	}
}
