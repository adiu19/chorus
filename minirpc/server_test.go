package minirpc

import (
	"net"
	"testing"
	"time"
)

func TestServerAcceptsConnection(t *testing.T) {
	s := NewServer()
	go s.Serve("127.0.0.1:0") //  port 0 tells the OS to pick a random free port, so tests never collide

	// wait for listener to be ready
	time.Sleep(50 * time.Millisecond)

	addr := s.ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// send a DATA frame
	err = WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameData,
		Method:   "SubmitJob",
		Payload:  []byte("test payload"),
	})
	if err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// send END_STREAM
	err = WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameEndStream,
	})
	if err != nil {
		t.Fatalf("WriteFrame END_STREAM: %v", err)
	}
}
