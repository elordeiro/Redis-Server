package main

import (
	"bufio"
	"io"
	"net"
	"sync"
)

// Constants ------------------------------------------------------------------
// RESP types
const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	NULL    = '_'
	RDB     = '@'
)

var CRLF = []byte("\r\n")

// Server roles
const (
	MASTER = iota
	REPLICA
	CLIENT
)

// ----------------------------------------------------------------------------

// Types ----------------------------------------------------------------------
type RESP struct {
	Type   byte
	Value  string
	Values []*RESP
}

type Buffer struct {
	reader *bufio.Reader
}

type Writer struct {
	writer io.Writer
}

// Config flags
type Config struct {
	Port       string
	IsReplica  bool
	MasterHost string
	MasterPort string
	Dir        string
	Dbfilename string
}

type StreamKV struct {
	Seq int
	Key string
	Val string
}

type ServerType int

// Connection reader and writer
type ConnRW struct {
	Type   ServerType
	Conn   net.Conn
	Reader *Buffer
	Writer *Writer
	Chan   chan *RESP
}

type Server struct {
	Role             ServerType
	Listener         net.Listener
	Redirect         bool
	NeedAcks         bool
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplid     string
	Dir              string
	Dbfilename       string
	MasterReplOffset int
	ReplicaCount     int
	MasterConn       net.Conn
	Conns            []*ConnRW
	SETs             map[string]string
	SETsMu           sync.RWMutex
	EXPs             map[string]int64
	XADDs            map[string]map[int64][]*StreamKV
	XADDsTop         map[string]int64
	XADDsMu          sync.RWMutex
}

// ----------------------------------------------------------------------------

// Reader and Writer constructors --------------------------------------------
func NewBuffer(rd io.Reader) *Buffer {
	return &Buffer{
		reader: bufio.NewReader(rd),
	}
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		writer: wr,
	}
}

// ----------------------------------------------------------------------------
