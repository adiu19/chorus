package minirpc

import (
	"log"
	"net"
	"sync"
)

// HandlerFunc is a type definition for a function that'll act on our stream
type HandlerFunc func(stream *Stream)

// Server encapsulates our server def
type Server struct {
	ln       net.Listener
	mu       sync.Mutex
	conns    map[*conn]struct{}
	handlers map[string]HandlerFunc
}

type conn struct {
	server        *Server
	nc            net.Conn
	writeCh       chan *Frame // a buffered channel that acts as the outbound queue.
	activeStreams map[uint32]*Stream
}

// NewServer creates an instance of the server
func NewServer() *Server {
	return &Server{
		conns:    make(map[*conn]struct{}),
		handlers: make(map[string]HandlerFunc),
	}
}

func (s *Server) newConn(nc net.Conn) *conn {
	return &conn{
		server:        s,
		nc:            nc,
		writeCh:       make(chan *Frame, 64),
		activeStreams: make(map[uint32]*Stream),
	}
}

// Handle registers a method into our server
func (s *Server) Handle(method string, fn HandlerFunc) {
	s.handlers[method] = fn
}

// serve() starts two loops:
//   - readLoop runs in its own goroutine — reads frames off the wire
//   - writeLoop runs on the current goroutine — drains writeCh to the wire
func (c *conn) serve() {
	go c.readLoop()
	c.writeLoop()
}

func (c *conn) writeLoop() {
	for f := range c.writeCh {
		if err := WriteFrame(c.nc, f); err != nil {
			log.Printf("minirpc: write error: %v", err)
			return
		}
	}
}

func (c *conn) readLoop() {
	// when the read loop exits (client disconnects or error), it cleans up everything:
	// 	- closes writeCh (which kills the write loop),
	// 	- closes the TCP socket,
	// 	- and removes the connection from the server's tracking map
	// The cleanup order matters:
	// 	- closing writeCh causes range c.writeCh in the write loop to exit
	// 	- so both goroutines shut down cleanly when the reader dies.
	defer func() {
		// close all read channels for streams
		for _, st := range c.activeStreams {
			close(st.recvChan)
		}
		close(c.writeCh) // close the write channel for the TCP conn
		c.nc.Close()
		c.server.mu.Lock()
		delete(c.server.conns, c)
		c.server.mu.Unlock()
		log.Printf("minirpc: connection closed: %s", c.nc.RemoteAddr())
	}()

	for {
		f, err := ReadFrame(c.nc)
		if err != nil {
			log.Printf("minirpc: read error: %v", err)
			return
		}
		log.Printf("minirpc: frame stream=%d type=%d method=%q len=%d",
			f.StreamID, f.Type, f.Method, len(f.Payload))

		st, exists := c.activeStreams[f.StreamID]
		if !exists {
			handler, ok := c.server.handlers[f.Method]
			if !ok {
				log.Printf("minirpc: unknown method: %q", f.Method)
				continue
			}

			st = &Stream{
				ID:       f.StreamID,
				recvChan: make(chan *Frame, 16),
				sendChan: c.writeCh, // all streams share the same write channel as the TCP conn
			}

			c.activeStreams[f.StreamID] = st
			go handler(st) // spawn a goroutine for the stream
		}

		st.recvChan <- f
	}
}

// Serve new TCP connections
func (s *Server) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	log.Printf("minirpc: listening on %s", addr)

	for {
		// net.Conn is bi-directional and serves both reads and writes
		nc, err := ln.Accept() // create a socket
		if err != nil {
			return err
		}
		c := s.newConn(nc)
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go c.serve() // new client showed up, deal with it
	}
}
