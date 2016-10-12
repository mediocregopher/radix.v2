package redis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"time"
)

const (
	maxBufferSize = 1 << 20 // 1 MB
)

// ErrPipelineEmpty is returned from PipeResp() to indicate that all commands
// which were put into the pipeline have had their responses read
var ErrPipelineEmpty = errors.New("pipeline queue empty")

// Client describes a Redis client.
type Client struct {
	conn         net.Conn
	respReader   *RespReader
	timeout      time.Duration
	pending      []request
	writeScratch []byte
	writeBuf     *bytes.Buffer

	completed, completedHead []*Resp

	// The network/address of the redis instance this client is connected to.
	// These will be whatever strings were passed into the Dial function when
	// creating this connection
	Network, Addr string

	// The most recent network error which occurred when either reading
	// or writing. A critical network error is basically any non-application
	// level error, e.g. a timeout, disconnect, etc... Close is automatically
	// called on the client when it encounters a critical network error
	//
	// NOTE: The ReadResp method does *not* consider a timeout to be a critical
	// network error, and will not set this field in the event of one. Other
	// methods which deal with a command-then-response (e.g. Cmd, PipeResp) do
	// set this and close the connection in the event of a timeout
	LastCritical error
}

// request describes a client's request to the redis server
type request struct {
	cmd  string
	args []interface{}
}

// DialTimeout connects to the given Redis server with the given timeout, which
// will be used as the read/write timeout when communicating with redis
func DialTimeout(network, addr string, timeout time.Duration) (*Client, error) {
	// establish a connection
	conn, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}

	completed := make([]*Resp, 0, 10)
	return &Client{
		conn:          conn,
		respReader:    NewRespReader(conn),
		timeout:       timeout,
		writeScratch:  make([]byte, 0, 128),
		writeBuf:      bytes.NewBuffer(make([]byte, 0, 128)),
		completed:     completed,
		completedHead: completed,
		Network:       network,
		Addr:          addr,
	}, nil
}

// Dial connects to the given Redis server.
func Dial(network, addr string) (*Client, error) {
	return DialTimeout(network, addr, time.Duration(0))
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Cmd calls the given Redis command.
func (c *Client) Cmd(cmd string, args ...interface{}) *Resp {
	err := c.writeRequest(request{cmd, args})
	if err != nil {
		return NewRespIOErr(err)
	}
	return c.readResp(true)
}

type bulkStringReader struct {
	r           io.Reader
	ContentSize int
	readSize    int // size user already read
}

func (b *bulkStringReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.readSize += n
	return n, err
}

func (b *bulkStringReader) discardRemainder() error {
	if b.r == nil {
		return nil
	}
	sizeRemain := b.ContentSize - b.readSize + 2 // +2 for trailing \r\n
	var buf []byte
	for sizeRemain > 0 {
		var n int
		var err error
		if sizeRemain >= maxBufferSize {
			if buf == nil {
				// allocates maxBufferSize at most
				buf = make([]byte, maxBufferSize)
			}
			n, err = b.r.Read(buf)
		} else {
			buf = make([]byte, sizeRemain)
			n, err = b.r.Read(buf)
		}

		if err != nil {
			return err
		}
		sizeRemain -= n
	}
	return nil
}

func newBulkStringReader(conn io.Reader) (*bulkStringReader, error) {
	buf := make([]byte, 1) // read protocol header byte by byte
	n, err := conn.Read(buf)
	if err != nil || n != 1 {
		return nil, errParse
	}
	if buf[0] != bulkStrPrefix[0] {
		return nil, errBadType
	}
	sizeBuf := make([]byte, 0)
FOR:
	for {
		n, err = conn.Read(buf)
		if err != nil || n != 1 {
			return nil, errParse
		}
		switch buf[0] {
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			sizeBuf = append(sizeBuf, buf[0])
		case delim[0]: // got '\r', continue to read '\n'
			n, err = conn.Read(buf)
			if err != nil || n != 1 || buf[0] != delim[1] {
				return nil, errParse
			}
			break FOR
		default:
			return nil, errParse
		}
	}

	size, err := strconv.Atoi(string(sizeBuf))
	if err != nil {
		return nil, errParse
	}
	if size < 0 {
		// a "null bulk string" if size < 0
		return nil, nil
	}
	return &bulkStringReader{
		r:           conn,
		ContentSize: size,
		readSize:    0,
	}, nil
}

// Suit for Redis commands return bulk strings, no buffer included
// Redis bulk string supports value up to 512MB, use io.Reader would reduce
// memory consumption and memory copy time
func (c *Client) RawResponseCmd(read func(io.Reader) error,
	cmd string, args ...interface{}) (readSize int, err error) {

	err = c.writeRequest(request{cmd, args})
	if err != nil {
		return 0, err
	}
	r, err := newBulkStringReader(c.conn)
	if err != nil {
		return 0, err
	}
	if r == nil {
		return 0, nil
	}

	limitedReader := io.LimitReader(r, int64(r.ContentSize))
	err = read(limitedReader)
	go func() {
		clientError := r.discardRemainder()
		if clientError != nil {
			c.LastCritical = clientError
			c.Close()
		}
	}()
	return r.readSize, err
}

// PipeAppend adds the given call to the pipeline queue.
// Use PipeResp() to read the response.
func (c *Client) PipeAppend(cmd string, args ...interface{}) {
	c.pending = append(c.pending, request{cmd, args})
}

// PipeResp returns the reply for the next request in the pipeline queue. Err
// with ErrPipelineEmpty is returned if the pipeline queue is empty.
func (c *Client) PipeResp() *Resp {
	if len(c.completed) > 0 {
		r := c.completed[0]
		c.completed = c.completed[1:]
		return r
	}

	if len(c.pending) == 0 {
		return NewResp(ErrPipelineEmpty)
	}

	nreqs := len(c.pending)
	err := c.writeRequest(c.pending...)
	c.pending = nil
	if err != nil {
		return NewRespIOErr(err)
	}
	c.completed = c.completedHead
	for i := 0; i < nreqs; i++ {
		r := c.readResp(true)
		c.completed = append(c.completed, r)
	}

	// At this point c.completed should have something in it
	return c.PipeResp()
}

// PipeClear clears the contents of the current pipeline queue, both commands
// queued by PipeAppend which have yet to be sent and responses which have yet
// to be retrieved through PipeResp. The first returned int will be the number
// of pending commands dropped, the second will be the number of pending
// responses dropped
func (c *Client) PipeClear() (int, int) {
	callCount, replyCount := len(c.pending), len(c.completed)
	if callCount > 0 {
		c.pending = nil
	}
	if replyCount > 0 {
		c.completed = nil
	}
	return callCount, replyCount
}

// ReadResp will read a Resp off of the connection without sending anything
// first (useful after you've sent a SUSBSCRIBE command). This will block until
// a reply is received or the timeout is reached (returning the IOErr). You can
// use IsTimeout to check if the Resp is due to a Timeout
//
// Note: this is a more low-level function, you really shouldn't have to
// actually use it unless you're writing your own pub/sub code
func (c *Client) ReadResp() *Resp {
	return c.readResp(false)
}

// strict indicates whether or not to consider timeouts as critical network
// errors
func (c *Client) readResp(strict bool) *Resp {
	if c.timeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	r := c.respReader.Read()
	if r.IsType(IOErr) && (strict || !IsTimeout(r)) {
		c.LastCritical = r.Err
		c.Close()
	}
	return r
}

type BulkStringWriter struct {
	WriteFunc   func(io.Writer) error
	ContentSize int64
}

func (b BulkStringWriter) write(w io.Writer) (written int64, err error) {
	lengthStr := []byte(strconv.FormatInt(b.ContentSize, 10))

	written, err = writeBytesHelper(w, bulkStrPrefix, written, err)
	written, err = writeBytesHelper(w, lengthStr, written, err)
	written, err = writeBytesHelper(w, delim, written, err)
	if err != nil {
		return
	}
	err = b.WriteFunc(w)
	written, err = writeBytesHelper(w, delim, written, err)
	return
}

func (c *Client) writeRequest(requests ...request) error {
	if c.timeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	var err error
outer:
	for i := range requests {
		c.writeBuf.Reset()
		elems := flattenedLength(requests[i].args...) + 1
		_, err = writeArrayHeader(c.writeBuf, c.writeScratch, int64(elems))
		if err != nil {
			break
		}

		_, err = writeTo(c.writeBuf, c.writeScratch, requests[i].cmd, true, true)
		if err != nil {
			break
		}

		for _, arg := range requests[i].args {
			switch a := arg.(type) {
			case BulkStringWriter:
				// flush buffer contents into socket first
				if _, err = c.writeBuf.WriteTo(c.conn); err != nil {
					break outer
				}
				c.writeBuf.Reset()

				if n, err := a.write(c.conn); err != nil || n != a.ContentSize {
					break outer
				}
			default:
				_, err = writeTo(c.writeBuf, c.writeScratch, arg, true, true)
				if err != nil {
					break outer
				}
			}
		}

		if _, err = c.writeBuf.WriteTo(c.conn); err != nil {
			break
		}
	}
	if err != nil {
		c.LastCritical = err
		c.Close()
		return err
	}
	return nil
}

var errBadCmdNoKey = errors.New("bad command, no key")

// KeyFromArgs is a helper function which other library packages which wrap this
// one might find useful. It takes in a set of arguments which might be passed
// into Cmd and returns the first key for the command. Since radix supports
// complicated arguments (like slices, slices of slices, maps, etc...) this is
// not always as straightforward as it might seem, so this helper function is
// provided.
//
// An error is returned if no key can be determined
func KeyFromArgs(args ...interface{}) (string, error) {
	if len(args) == 0 {
		return "", errBadCmdNoKey
	}
	arg := args[0]
	switch argv := arg.(type) {
	case string:
		return argv, nil
	case []byte:
		return string(argv), nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			argVal := reflect.ValueOf(arg)
			if argVal.Len() < 1 {
				return "", errBadCmdNoKey
			}
			first := argVal.Index(0).Interface()
			return KeyFromArgs(first)
		case reflect.Map:
			// Maps have no order, we can't possibly choose a key out of one
			return "", errBadCmdNoKey
		default:
			return fmt.Sprint(arg), nil
		}
	}
}
