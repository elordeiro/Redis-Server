package main

import (
	"testing"
)

func TestPing(t *testing.T) {
	createMasterServer("6379")
	conn := connectToServer("6379")
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

func TestEcho(t *testing.T) {
	createMasterServer("6379")
	conn := connectToServer("6379")
	defer conn.Conn.Close()

	Write(conn.Writer, ToResp("ECHO", "Hello World"))
	parsedResp, _, err := conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Type != STRING || parsedResp.Value != "Hello World" {
		t.Errorf("Expected ECHO response, got %v", parsedResp)
	}
}

func TestSet(t *testing.T) {
	createMasterServer("6379")
	conn := connectToServer("6379")
	defer conn.Conn.Close()

	Write(conn.Writer, ToResp("SET", "foo", "bar"))
	parsedResp, _, err := conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if !parsedResp.IsOkay() {
		t.Errorf("Expected OK response, got %v", parsedResp)
	}
}

func TestGet(t *testing.T) {
	createMasterServer("6379")
	conn := connectToServer("6379")
	defer conn.Conn.Close()

	Write(conn.Writer, ToResp("SET", "foo", "bar"))
	parsedResp, _, _ := conn.Buffer.Read()
	if !parsedResp.IsOkay() {
		t.Errorf("Expected OK response, got %v", parsedResp)
	}

	Write(conn.Writer, ToResp("GET", "foo"))
	parsedResp, _, err := conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Type != STRING || parsedResp.Value != "bar" {
		t.Errorf("Expected GET response, got %v", parsedResp)
	}
}

func TestXadd(t *testing.T) {
	createMasterServer("6379")
	conn := connectToServer("6379")
	defer conn.Conn.Close()

	Write(conn.Writer, ToResp("XADD", "stream", "0-1", "key", "value"))
	parsedResp, _, err := conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Type != BULK || parsedResp.Value != "0-1" {
		t.Errorf("Expected XADD response, got %v", parsedResp)
	}

	Write(conn.Writer, ToResp("XADD", "stream", "*", "foo", "bar"))
	parsedResp, _, err = conn.Buffer.Read()
	if err != nil {
		t.Errorf("Failed to read response: %v", err)
	}
	if parsedResp.Type != BULK {
		t.Errorf("Expected XADD response, got %v", parsedResp)
	}
}
