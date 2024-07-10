package main

import (
	"net"
	"testing"
	"time"
)

type ReadWriter struct {
	Conn net.Conn
	*Buffer
	*Writer
}

func connectToServer(port string) *ReadWriter {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", "localhost:"+port)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return &ReadWriter{
			Conn:   conn,
			Buffer: NewBuffer(conn),
			Writer: NewWriter(conn),
		}
	}
	return nil
}

func createMasterServer(port string) {
	go func() {
		server, err := NewServer(&Config{Port: port})
		if err != nil {
			panic(err)
		}
		server.serverListen()
	}()
}

func createReplicaServer(port, masterPort string) {
	go func() {
		server, err := NewServer(&Config{
			Port:       port,
			IsReplica:  true,
			MasterHost: "localhost",
			MasterPort: masterPort,
		})
		if err != nil {
			panic(err)
		}
		err = server.handShake()
		if err != nil {
			panic(err)
		}
		server.serverListen()
	}()
}

func TestNewServer(t *testing.T) {
	createMasterServer("6379")

	// Ping the server to check if it is running
	conn := connectToServer("6379")
	if conn == nil || conn.Conn == nil {
		t.Errorf("Failed to connect to server")
	}
	defer conn.Conn.Close()

	Write(conn.Writer, PingResp())
	parsedResp, _, err := conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if !parsedResp.IsPong() {
		t.Errorf("Expected PONG response, got %v", parsedResp)
	}
}

func TestNewReplica(t *testing.T) {
	// Create master server and connect to it
	createMasterServer("6379")
	masterConn := connectToServer("6379")
	if masterConn == nil || masterConn.Conn == nil {
		t.Errorf("Failed to connect to server")
	}
	defer masterConn.Conn.Close()

	// Create replica server and connect to it
	createReplicaServer("6380", "6379")
	replConn := connectToServer("6380")
	if replConn == nil || replConn.Conn == nil {
		t.Errorf("Failed to connect to server")
	}
	defer replConn.Conn.Close()

	// Ping replica server
	Write(replConn.Writer, PingResp())
	parsedResp, _, err := replConn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if !parsedResp.IsPong() {
		t.Errorf("Expected PONG response, got %v", parsedResp)
	}

	// Check if set command is propagated to replica
	// Set key foo to bar in master
	Write(masterConn.Writer, ToResp("SET", "foo", "bar"))
	parsedResp, _, _ = masterConn.Buffer.Read()
	if !parsedResp.IsOkay() {
		t.Errorf("Expected OK response, got %v", parsedResp)
	}
	// Wait for replication
	Write(masterConn.Writer, ToResp("WAIT", "1", "2000"))
	parsedResp, _, err = masterConn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Type != INTEGER {
		t.Errorf("Expected integer response, got %v", parsedResp)
	}
	if parsedResp.Value != "1" {
		t.Errorf("Expected 1, got %v", parsedResp.Value)
	}

	// Get key foo in replica
	Write(replConn.Writer, ToResp("GET", "foo"))
	parsedResp, _, err = replConn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Value != "bar" {
		t.Errorf("Expected bar, got %v", parsedResp)
	}
}
