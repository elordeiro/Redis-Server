package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	MASTER = iota
	SLAVE
)

type serverType int

type Server struct {
	Role       serverType
	Listener   net.Listener
	Conn       []net.Conn
	MasterHost string
	MasterPort string
}

// Globals --------------------------------------------------------------------
var ThisServer *Server
var Flags map[string]string

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

func NewServer() (*Server, error) {
	port := "6379"
	if val, ok := Flags["port"]; ok {
		port = val
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		return nil, err
	}

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

	return &Server{
		Listener:   l,
		Conn:       make([]net.Conn, 0),
		Role:       role,
		MasterHost: masterHost,
		MasterPort: masterPort,
	}, nil
}

func (s *Server) serverAccept() {
	conn, err := s.Listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		return
	}
	s.Conn = append(s.Conn, conn)

	go s.handleConnection(conn)
}

func (s *Server) serverClose() {
	for _, conn := range s.Conn {
		conn.Close()
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	for {
		resp := NewBuffer(conn)

		parsedResp, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Closing")
			return
		}

		result := Handler(parsedResp)
		writer := NewWriter(conn)
		writer.Write(result)
	}
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

	fmt.Println("listening on port: " + ThisServer.MasterPort + "...")

	defer ThisServer.serverClose()

	for {
		ThisServer.serverAccept()
	}
}
