package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

const (
	MASTER = iota
	SLAVE
)

type serverType int

type Server struct {
	Role             serverType
	Listener         net.Listener
	Conn             []net.Conn
	Writers          []*Writer
	Port             string
	MasterHost       string
	MasterPort       string
	MasterConn       net.Conn
	MasterReplid     string
	MasterReplOffset int
}

// Globals --------------------------------------------------------------------
var ThisServer *Server
var Flags map[string]string
var ResponseBuf = []*RESP{}

// ----------------------------------------------------------------------------

func (st serverType) String() string {
	switch st {
	case MASTER:
		return "master"
	case SLAVE:
		return "slave"
	default:
		return "unknown"
	}
}

// Handshake happens in 3 stages
func (s *Server) handShake() error {
	conn, err := net.Dial("tcp", s.MasterHost+":"+s.MasterPort)
	ThisServer.MasterConn = conn
	if err != nil {
		fmt.Println("Failed to connect to master")
		os.Exit(1)
	}

	resp := NewBuffer(conn)
	writer := NewWriter(conn)

	// Stage 1
	writer.Write(PingResp())
	parsedResp, err := resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsPong() {
		return errors.New("master server did not respond with PONG")
	}

	// Stage 2
	writer.Write(ReplconfResp(1))
	parsedResp, err = resp.Read()
	if err != nil {
		return err
	}
	if !parsedResp.IsOkay() {
		return errors.New("master server did not respond with OK")
	}

	writer.Write(ReplconfResp(2))
	parsedResp, err = resp.Read()
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

	return nil
}

// Generate Unique server id --------------------------------------------------
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

func NewServer() (*Server, error) {
	// Set server port number
	port := "6379"
	if val, ok := Flags["port"]; ok {
		port = val
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port " + port)
		return nil, err
	}

	// Set server role, master host and master port
	var role serverType = MASTER
	masterHost, masterPort := "localhost", port
	if val, ok := Flags["replicaof"]; ok {
		role = SLAVE
		hostAndPort := strings.Split(val, " ")
		if len(hostAndPort) != 2 {
			return nil, fmt.Errorf("invalid option for --replicaof")
		}
		masterHost, masterPort = hostAndPort[0], hostAndPort[1]
	}

	// Set server repl id and repl offset
	replId := RandStringBytes(40)

	return &Server{
		Listener:         l,
		Conn:             make([]net.Conn, 0),
		Role:             role,
		Port:             port,
		MasterHost:       masterHost,
		MasterPort:       masterPort,
		MasterReplid:     replId,
		MasterReplOffset: 0,
		Writers:          []*Writer{},
	}, nil
}

func (s *Server) serverAccept() {
	conn, err := s.Listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		return
	}
	s.Conn = append(s.Conn, conn)

	if ThisServer.Role == MASTER {
		go s.handleConnectionMaster(conn)
	} else {
		go s.handleConnectionSlave(conn)
	}
}

func (s *Server) serverClose() {
	for _, conn := range s.Conn {
		conn.Close()
	}
}

func (s *Server) handleConnectionMaster(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)

	for {
		parsedResp, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Closing")
			return
		}

		results := writer.Handler(parsedResp)

		for _, result := range results {
			writer.Write(result)
		}
	}
}

func (s *Server) handleClientConnection(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
	for {
		parsedResp, err := resp.Read()
		var results []*RESP
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Closing")
				return
			}
			fmt.Println(err)
		} else {
			results = writer.Handler(parsedResp)
		}

		for _, result := range results {
			writer.Write(result)
		}
	}
}

func (s *Server) handleMasterConnection(conn net.Conn) {
	resp := NewBuffer(conn)
	writer := NewWriter(conn)
	for {
		parsedResp, err := resp.Read()
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("Closing")
				return
			}
			fmt.Println(err)
		} else {
			writer.Handler(parsedResp)
		}
	}
}

func (s *Server) handleConnectionSlave(conn net.Conn) {
	go s.handleClientConnection(conn)
	go s.handleMasterConnection(ThisServer.MasterConn)
}

func getCommandLineArgs() error {
	Flags = map[string]string{}
	for i := 1; i < len(os.Args); i += 2 {
		flag := strings.TrimPrefix(os.Args[i], "--")
		if i+1 == len(os.Args) {
			return fmt.Errorf("no option for %v flag", flag)
		}
		opt := os.Args[i+1]
		Flags[flag] = opt
	}
	return nil
}

func main() {
	err := getCommandLineArgs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ThisServer, err = NewServer()
	if err != nil {
		fmt.Println("Failed to create server")
		os.Exit(1)
	}
	defer ThisServer.serverClose()

	if ThisServer.Role == SLAVE {
		err := ThisServer.handShake()
		if err != nil {
			_ = fmt.Errorf("failed to connect to master server")
			os.Exit(1)
		}
	}

	fmt.Println("listening on port: " + ThisServer.Port + "...")

	for {
		ThisServer.serverAccept()
	}
}
