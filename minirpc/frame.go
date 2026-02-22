package minirpc

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FrameType represents the type of data contained within a frame, we use a single byte to represent this
type FrameType uint8

// 0x01 = there's real data here, read the payload
// 0x02 = stream is over, nothing more to read
// 0x03 = something broke, the payload is an error message
const (
	FrameData      FrameType = 0x01
	FrameEndStream FrameType = 0x02
	FrameError     FrameType = 0x03
)

// Frame is a unit of data transmitted over a TCP stream.
type Frame struct {
	StreamID uint32
	Type     FrameType
	Method   string
	Payload  []byte
}

// WriteFrame serializes each field sequentially in big-endian. Method length is capped at 255 (fits in 1 byte).
func WriteFrame(w io.Writer, f *Frame) error {
	// stream_id: 4 bytes
	if err := binary.Write(w, binary.BigEndian, f.StreamID); err != nil {
		return fmt.Errorf("write stream_id: %w", err)
	}

	// type: 1 byte
	if err := binary.Write(w, binary.BigEndian, f.Type); err != nil {
		return fmt.Errorf("write type: %w", err)
	}

	// method_len: 1 byte + method: M bytes
	methodBytes := []byte(f.Method)
	if len(methodBytes) > 255 {
		return fmt.Errorf("method name too long: %d bytes", len(methodBytes))
	}
	if err := binary.Write(w, binary.BigEndian, uint8(len(methodBytes))); err != nil {
		return fmt.Errorf("write method_len: %w", err)
	}
	if len(methodBytes) > 0 {
		if _, err := w.Write(methodBytes); err != nil {
			return fmt.Errorf("write method: %w", err)
		}
	}

	// payload_len: 4 bytes + payload: N bytes
	if err := binary.Write(w, binary.BigEndian, uint32(len(f.Payload))); err != nil {
		return fmt.Errorf("write payload_len: %w", err)
	}
	if len(f.Payload) > 0 {
		if _, err := w.Write(f.Payload); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
	}

	return nil
}

// ReadFrame deserializes in the same order. Uses io.ReadFull for variable-length fields to guarantee we read exactly the right number of bytes (plain Read can return short)
func ReadFrame(r io.Reader) (*Frame, error) {
	f := &Frame{}

	// stream_id: 4 bytes
	if err := binary.Read(r, binary.BigEndian, &f.StreamID); err != nil {
		return nil, fmt.Errorf("read stream_id: %w", err)
	}

	// type: 1 byte
	if err := binary.Read(r, binary.BigEndian, &f.Type); err != nil {
		return nil, fmt.Errorf("read type: %w", err)
	}

	// method_len: 1 byte
	var methodLen uint8
	if err := binary.Read(r, binary.BigEndian, &methodLen); err != nil {
		return nil, fmt.Errorf("read method_len: %w", err)
	}

	// method: M bytes
	if methodLen > 0 {
		methodBytes := make([]byte, methodLen)
		if _, err := io.ReadFull(r, methodBytes); err != nil {
			return nil, fmt.Errorf("read method: %w", err)
		}
		f.Method = string(methodBytes)
	}

	// payload_len: 4 bytes
	var payloadLen uint32
	if err := binary.Read(r, binary.BigEndian, &payloadLen); err != nil {
		return nil, fmt.Errorf("read payload_len: %w", err)
	}

	// payload: N bytes
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
	}

	return f, nil
}
