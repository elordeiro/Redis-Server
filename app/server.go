package main

import (
	"fmt"
	"net"
	"os"
)

const (
	MASTER = iota
	SLAVE
)

type serverType int

type Server struct {
	Type     serverType
	Listener net.Listener
	Conn     []net.Conn
}

// Globals --------------------------------------------------------------------
var ThisServer Server

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

func NewServer(address string) (*Server, error) {
	l, err := net.Listen("tcp", "0.0.0.0:"+address)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		return nil, err
	}

	var typ serverType
	if address == "6379" {
		typ = MASTER
	} else {
		typ = SLAVE
	}

	return &Server{
		Listener: l,
		Conn:     make([]net.Conn, 0),
		Type:     typ,
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

func getCommandLineArgs() (string, error) {
	if len(os.Args) == 1 {
		return "6379", nil
	}
	if args := os.Args[1]; args != "--port" {
		return "", fmt.Errorf("invalid argument: %s", args)
	}
	return os.Args[2], nil
}

func main() {
	address, err := getCommandLineArgs()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ThisServer, err := NewServer(address)
	if err != nil {
		fmt.Println("Failed to create server")
		os.Exit(1)
	}

	fmt.Println("listening on port: " + address + "...")

	defer ThisServer.serverClose()

	for {
		ThisServer.serverAccept()
	}
}
