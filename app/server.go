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

	"math/rand/v2"

	"github.com/codecrafters-io/redis-starter-go/radix"
)

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
		EXPs:             map[string]int64{},
		XADDs:            map[string]*radix.Radix{},
		XADDsMu:          sync.RWMutex{},
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
	if config.Dir != "" && config.Dbfilename != "" {
		server.Dir = config.Dir
		server.Dbfilename = config.Dbfilename
		server.LoadRDB()

	}

	return server, nil
}

// Generate random string for repl id
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func initRandom() {
	// rand.Seed(uint64(time.Now().UnixNano()))
	rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))
}

func RandStringBytes(n int) string {
	initRandom()
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int64()%int64(len(letterBytes))]
	}
	return string(b)
}

func (s *Server) LoadRDB() {
	// Check if directory exists
	if _, err := os.Stat(s.Dir); os.IsNotExist(err) {
		fmt.Println("Directory does not exist")
		return
	}

	// Check if file exists
	if _, err := os.Stat(s.Dir + "/" + s.Dbfilename); os.IsNotExist(err) {
		fmt.Println("File does not exist")
		return
	}

	// Open file and read contents
	file, err := os.Open(s.Dir + "/" + s.Dbfilename)
	if err != nil {
		fmt.Println("Failed to open file")
		return
	}
	defer file.Close()

	s.decodeRDB(NewBuffer(file))
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
	connRW := &ConnRW{MASTER, conn, resp, writer, nil}

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
	rdb, err := resp.ReadFullResync()
	if err != nil {
		return err
	}
	s.Handler(rdb, connRW)

	s.MasterReplOffset = 0
	go s.handleMasterConnAsReplica(connRW)

	return nil
}

func (s *Server) serverClose() {
	for _, conn := range s.Conns {
		conn.Conn.Close()
	}
}

// ----------------------------------------------------------------------------

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
				fmt.Println("Writing response", result)
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

func (s *Server) handleMasterConnAsReplica(connRW *ConnRW) {
	s.Conns = append(s.Conns, connRW)
	for {
		fmt.Println("Handling master connection")
		parsedResp, n, err := connRW.Reader.Read()
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
