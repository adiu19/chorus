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

func TestStreamHandler(t *testing.T) {
	s := NewServer()
	s.Handle("Stream", StreamHandler())
	go s.Serve("127.0.0.1:0")
	time.Sleep(50 * time.Millisecond)

	addr := s.ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameData,
		Method:   "Stream",
		Payload:  []byte("the quick brown fox"),
	})
	WriteFrame(conn, &Frame{
		StreamID: 1,
		Type:     FrameEndStream,
	})

	expected := []string{"the", "quick", "brown", "fox"}
	for _, want := range expected {
		f, err := ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if f.Type != FrameData {
			t.Fatalf("Type = %d, want DATA", f.Type)
		}
		if string(f.Payload) != want {
			t.Errorf("Payload = %q, want %q", f.Payload, want)
		}
	}

	end, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame END_STREAM: %v", err)
	}
	if end.Type != FrameEndStream {
		t.Errorf("Type = %d, want %d", end.Type, FrameEndStream)
	}
}

func TestMultiplexing(t *testing.T) {
	s := NewServer()
	s.Handle("Stream", StreamHandler())
	go s.Serve("127.0.0.1:0")
	time.Sleep(50 * time.Millisecond)

	addr := s.ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	WriteFrame(conn, &Frame{StreamID: 1, Type: FrameData, Method: "Stream", Payload: []byte("hello world")})
	WriteFrame(conn, &Frame{StreamID: 1, Type: FrameEndStream})
	WriteFrame(conn, &Frame{StreamID: 3, Type: FrameData, Method: "Stream", Payload: []byte("foo bar baz")})
	WriteFrame(conn, &Frame{StreamID: 3, Type: FrameEndStream})

	stream1 := []*Frame{}
	stream3 := []*Frame{}
	for len(stream1) == 0 || stream1[len(stream1)-1].Type != FrameEndStream ||
		len(stream3) == 0 || stream3[len(stream3)-1].Type != FrameEndStream {
		f, err := ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		switch f.StreamID {
		case 1:
			stream1 = append(stream1, f)
		case 3:
			stream3 = append(stream3, f)
		default:
			t.Fatalf("unexpected stream ID: %d", f.StreamID)
		}
	}

	// stream 1: "hello world" → 2 chunks + END_STREAM
	if len(stream1) != 3 {
		t.Fatalf("stream 1: got %d frames, want 3", len(stream1))
	}
	for i, want := range []string{"hello", "world"} {
		if string(stream1[i].Payload) != want {
			t.Errorf("stream 1 frame %d = %q, want %q", i, stream1[i].Payload, want)
		}
	}

	// stream 3: "foo bar baz" → 3 chunks + END_STREAM
	if len(stream3) != 4 {
		t.Fatalf("stream 3: got %d frames, want 4", len(stream3))
	}
	for i, want := range []string{"foo", "bar", "baz"} {
		if string(stream3[i].Payload) != want {
			t.Errorf("stream 3 frame %d = %q, want %q", i, stream3[i].Payload, want)
		}
	}
}
