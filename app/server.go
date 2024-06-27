package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/rand"
)

// Config flags ---------------------------------------------------------------
type Config struct {
	Port       string
	IsReplica  bool
	MasterHost string
	MasterPort string
	Dir        string
	Dbfilename string
}

// ----------------------------------------------------------------------------

// Server types ---------------------------------------------------------------
const (
	MASTER = iota
	REPLICA
	CLIENT
)

type ServerType int

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
}

func (st ServerType) String() string {
	switch st {
	case MASTER:
		return "master"
	case REPLICA:
		return "slave"
	default:
		return "unknown"
	}
}

// ----------------------------------------------------------------------------

// Server creation ------------------------------------------------------------
func NewServer(config *Config) (*Server, error) {
	server := &Server{
		Role:             MASTER,
		Port:             config.Port,
		MasterReplOffset: 0,
		Conns:            []*ConnRW{},
		SETs:             map[string]string{},
		SETsMu:           sync.RWMutex{},
	}

	// Set server port number
	l, err := net.Listen("tcp", "0.0.0.0:"+config.Port)
	if err != nil {
		fmt.Println("Failed to bind to port " + config.Port)
		return nil, err
	}
	server.Listener = l

	// Set server role, master host and master port
	if config.IsReplica {
		server.Role = REPLICA
		server.MasterHost = config.MasterHost
		server.MasterPort = config.MasterPort
	}

	// Set server repl id and repl offset
	server.MasterReplid = RandStringBytes(40)

	// Set Dir and Dbfilename if given
	if config.Dir != "" {
		server.Dir = config.Dir
	} else {
		server.Dir = "/tmp/redis-files"
	}
	if config.Dbfilename != "" {
		server.Dbfilename = config.Dbfilename
	} else {
		server.Dbfilename = "dump.rbd"
	}

	return server, nil
}

// Generate random string for repl id
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func initRandom() {
	rand.Seed(uint64(time.Now().UnixNano()))
}

func RandStringBytes(n int) string {
	initRandom()
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

// ----------------------------------------------------------------------------

// Accept / Handshake / Close connection --------------------------------------
func (s *Server) serverAccept() {
	conn, err := s.Listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		return
	}

	if s.Role == MASTER {
		go s.handleClientConnAsMaster(conn)
	} else {
		go s.handleClientConnAsReplica(conn)
	}
}

// Handshake happens in 3 stages
func (s *Server) handShake() error {
	conn, err := net.Dial("tcp", s.MasterHost+":"+s.MasterPort)
	s.MasterConn = conn
	if err != nil {
		fmt.Println("Failed to connect to master")
		os.Exit(1)
	}

	resp := NewBuffer(conn)
	writer := NewWriter(conn)

	// Stage 1
	Write(writer, PingResp())
	parsedResp, _, err := resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsPong() {
		return errors.New("master server did not respond with PONG")
	}

	// Stage 2
	Write(writer, ReplconfResp(1, s.Port))
	parsedResp, _, err = resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsOkay() {
		return errors.New("master server did not respond with OK")
	}

	Write(writer, ReplconfResp(2, s.Port))
	parsedResp, _, err = resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsOkay() {
		return errors.New("master server did not respond with OK")
	}

	// Stage 3
	Write(writer, Psync(0, 0))
	_, err = resp.ReadFullResync()
	if err != nil {
		return err
	}

	s.MasterReplOffset = 0
	go s.handleMasterConnAsReplica(conn)

	return nil
}

func (s *Server) serverClose() {
	for _, conn := range s.Conns {
		conn.Conn.Close()
	}
}

// ----------------------------------------------------------------------------

/*
Tomorrows task:
- Figure out why I needed to use a channel when no new goroutine was created
- Add a Read field to the ConnRW struct and use it to determine if the replica has any pending data to be read
*/

// Handle connection ----------------------------------------------------------
func (s *Server) handleClientConnAsMaster(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
	ch := make(chan *RESP)
	connRW := &ConnRW{CLIENT, conn, resp, writer, ch}
	s.Conns = append(s.Conns, connRW)
	for {
		parsedResp, _, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Closing")
			return
		}

		if s.Redirect {
			fmt.Println("Handling client connection on redirect", parsedResp)
			connRW.Chan <- parsedResp
		} else {
			fmt.Println("Handling client connection on main loop", parsedResp)
			results := s.Handler(parsedResp, connRW)

			for _, result := range results {
				Write(writer, result)
			}
		}
	}
}

func (s *Server) handleClientConnAsReplica(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
	connRW := &ConnRW{CLIENT, conn, resp, writer, nil}
	s.Conns = append(s.Conns, connRW)
	for {
		parsedResp, n, err := resp.Read()
		var results []*RESP
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Closing")
				return
			}
			fmt.Println(err)
		} else {
			results = s.Handler(parsedResp, connRW)
			s.MasterReplOffset += n
		}

		for _, result := range results {
			Write(writer, result)
		}
	}
}

func (s *Server) handleMasterConnAsReplica(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
	connRW := &ConnRW{MASTER, conn, resp, writer, nil}
	s.Conns = append(s.Conns, connRW)
	for {
		fmt.Println("Handling master connection")
		parsedResp, n, err := resp.Read()
		fmt.Println("Read: ", parsedResp)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Closing")
				return
			}
			fmt.Println("Error: ", err)
		} else {
			s.Handler(parsedResp, connRW)
			s.MasterReplOffset += n
		}
	}
}

// ----------------------------------------------------------------------------

// Entry point and command line arguments -------------------------------------
func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	server, err := NewServer(config)
	if err != nil {
		fmt.Println("Failed to create server")
		os.Exit(1)
	}
	defer server.serverClose()

	if server.Role == REPLICA {
		err := server.handShake()
		if err != nil {
			fmt.Println("failed to connect to master server")
			os.Exit(1)
		}
	}

	fmt.Println("listening on port: " + server.Port + "...")

	for {
		server.serverAccept()
	}
}

func parseFlags() (*Config, error) {
	config := &Config{}
	flag.StringVar(&config.Port, "port", "6379", "Server Port")
	repl := ""
	flag.StringVar(&repl, "replicaof", "", "Master connection <address port> to replicate")
	flag.StringVar(&config.Dir, "dir", "", "directory to rdb file")
	flag.StringVar(&config.Dbfilename, "dbfilename", "", "rdb file name")

	flag.Parse()

	if repl != "" {
		config.IsReplica = true
		ap := strings.Split(repl, " ")
		if len(ap) != 2 {
			return nil, errors.New("wrong argument count for -- replicaof")
		}
		config.MasterHost, config.MasterPort = ap[0], ap[1]
	}
	return config, nil
}
