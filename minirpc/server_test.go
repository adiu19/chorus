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

func TestStreamDispatch(t *testing.T) {
	s := NewServer()
	s.Handle("Echo", func(stream *Stream) {
		f, err := stream.Recv()
		if err != nil {
			return
		}
		stream.Send(&Frame{
			Type:    FrameData,
			Payload: f.Payload,
		})
		stream.Send(&Frame{Type: FrameEndStream})
	})

	go s.Serve("127.0.0.1:0")
	time.Sleep(50 * time.Millisecond)

	addr := s.ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// send request
	WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameData,
		Method:   "Echo",
		Payload:  []byte("ping"),
	})
	WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameEndStream,
	})

	// read response
	resp, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if string(resp.Payload) != "ping" {
		t.Errorf("Payload = %q, want %q", resp.Payload, "ping")
	}

	end, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame END_STREAM: %v", err)
	}
	if end.Type != FrameEndStream {
		t.Errorf("Type = %d, want %d", end.Type, FrameEndStream)
	}
}
