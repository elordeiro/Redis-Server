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
}

// ----------------------------------------------------------------------------

// Server types ---------------------------------------------------------------
const (
	MASTER = iota
	REPLICA
)

type ServerType int

type Server struct {
	Role             ServerType
	Listener         net.Listener
	Port             string
	MasterHost       string
	MasterPort       string
	MasterReplid     string
	MasterReplOffset int
	MasterConn       net.Conn
	Writers          []*Writer
	Conn             []net.Conn
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
		Writers:          []*Writer{},
		Conn:             []net.Conn{},
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

// Accept Handshake and  Close connection -------------------------------------
func (s *Server) serverAccept() {
	conn, err := s.Listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		return
	}
	s.Conn = append(s.Conn, conn)

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
	writer.Write(PingResp())
	parsedResp, _, err := resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsPong() {
		return errors.New("master server did not respond with PONG")
	}

	// Stage 2
	writer.Write(ReplconfResp(1, s.Port))
	parsedResp, _, err = resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsOkay() {
		return errors.New("master server did not respond with OK")
	}

	writer.Write(ReplconfResp(2, s.Port))
	parsedResp, _, err = resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsOkay() {
		return errors.New("master server did not respond with OK")
	}

	// Stage 3
	writer.Write(Psync(0, 0))
	_, err = resp.ReadFullResync()
	if err != nil {
		return err
	}

	s.MasterReplOffset = 0
	go s.handleMasterConnAsReplica(resp, writer)

	return nil
}

func (s *Server) serverClose() {
	for _, conn := range s.Conn {
		conn.Close()
	}
}

// ----------------------------------------------------------------------------

// Handle connection ----------------------------------------------------------
func (s *Server) handleClientConnAsMaster(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)

	for {
		parsedResp, _, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Closing")
			return
		}

		results := s.Handler(parsedResp, writer)

		for _, result := range results {
			writer.Write(result)
		}
	}
}

func (s *Server) handleClientConnAsReplica(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
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
			results = s.Handler(parsedResp, writer)
			s.MasterReplOffset += n
		}

		for _, result := range results {
			writer.Write(result)
		}
	}
}

func (s *Server) handleMasterConnAsReplica(resp *Buffer, writer *Writer) {
	for {
		fmt.Println("Handling master connection")
		parsedResp, n, err := resp.Read()
		fmt.Println(parsedResp)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Closing")
				return
			}
			fmt.Println(err)
		} else {
			s.Handler(parsedResp, writer)
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
