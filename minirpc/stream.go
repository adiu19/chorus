package minirpc

import (
	"fmt"
	"io"
)

// Stream encapsulates one stream per TCP connection
type Stream struct {
	ID       uint32
	recvChan chan *Frame
	sendChan chan *Frame
}

// Recv receives a frame from the receive channel
func (s *Stream) Recv() (*Frame, error) {
	f, ok := <-s.recvChan
	if !ok || f.Type == FrameEndStream {
		return nil, io.EOF
	}

	if f.Type == FrameError {
		return nil, fmt.Errorf("remote error: %s", string(f.Payload))
	}

	return f, nil
}

// Send sends a frame over the send channel defined in the stream
func (s *Stream) Send(f *Frame) error {
	f.StreamID = s.ID
	s.sendChan <- f
	return nil
}
