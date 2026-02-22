package minirpc

import (
	"bytes"
	"testing"
)

func TestRoundTripDataFrame(t *testing.T) {
	original := &Frame{
		StreamID: 1,
		Type:     FrameData,
		Method:   "SubmitJob",
		Payload:  []byte("hello world"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if got.StreamID != original.StreamID {
		t.Errorf("StreamID = %d, want %d", got.StreamID, original.StreamID)
	}
	if got.Type != original.Type {
		t.Errorf("Type = %d, want %d", got.Type, original.Type)
	}
	if got.Method != original.Method {
		t.Errorf("Method = %q, want %q", got.Method, original.Method)
	}
	if !bytes.Equal(got.Payload, original.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, original.Payload)
	}
}

func TestRoundTripEndStream(t *testing.T) {
	original := &Frame{
		StreamID: 5,
		Type:     FrameEndStream,
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if got.StreamID != original.StreamID {
		t.Errorf("StreamID = %d, want %d", got.StreamID, original.StreamID)
	}
	if got.Type != FrameEndStream {
		t.Errorf("Type = %d, want %d", got.Type, FrameEndStream)
	}
	if got.Method != "" {
		t.Errorf("Method = %q, want empty", got.Method)
	}
	if len(got.Payload) != 0 {
		t.Errorf("Payload = %q, want empty", got.Payload)
	}
}

func TestRoundTripErrorFrame(t *testing.T) {
	original := &Frame{
		StreamID: 3,
		Type:     FrameError,
		Payload:  []byte("something went wrong"),
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, original); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if got.StreamID != original.StreamID {
		t.Errorf("StreamID = %d, want %d", got.StreamID, original.StreamID)
	}
	if got.Type != FrameError {
		t.Errorf("Type = %d, want %d", got.Type, FrameError)
	}
	if got.Method != "" {
		t.Errorf("Method = %q, want empty", got.Method)
	}
	if !bytes.Equal(got.Payload, original.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, original.Payload)
	}
}
