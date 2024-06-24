package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Predefined responses -------------------------------------------------------
func OkResp() *RESP {
	return &RESP{Type: STRING, Value: "OK"}
}

func NullResp() *RESP {
	return &RESP{Type: NULL}
}

// Can be used for handshake stage 1
func PingResp() *RESP {
	return &RESP{
		Type: ARRAY,
		Values: []*RESP{
			{
				Type: BULK, Value: "PING",
			},
		},
	}
}

// Can be used for handshake stage 2
func ReplconfResp(i int) *RESP {
	switch i {
	case 1:
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "listening-port"},
				{Type: BULK, Value: ThisServer.Port},
			},
		}
	case 2:
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "capa"},
				{Type: BULK, Value: "psync2"},
			},
		}
	default:
		return NullResp()
	}

}

// Can be used for handshake stage 3
func Psync(replId, offset int) *RESP {
	replIdStr, offsetStr := "", ""
	switch replId {
	case 0:
		replIdStr, offsetStr = "?", "-1"
	default:
		replIdStr = strconv.Itoa(replId)
		offsetStr = strconv.Itoa(offset)
	}

	return &RESP{
		Type: ARRAY,
		Values: []*RESP{
			{Type: BULK, Value: "PSYNC"},
			{Type: BULK, Value: replIdStr},
			{Type: BULK, Value: offsetStr},
		},
	}
}

// ----------------------------------------------------------------------------

// Assert Responses -----------------------------------------------------------
func (resp *RESP) IsOkay() bool {
	if resp.Type != STRING {
		return false
	}
	if resp.Value != "OK" {
		return false
	}
	return true
}

func (resp *RESP) IsPong() bool {
	if resp.Type != STRING {
		return false
	}
	if resp.Value != "PONG" {
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

// Common commands -------------------------------------------------------------
func commandFunc() *RESP {
	return &RESP{Type: NULL, Value: "Command"}
}

func ping(args []*RESP) *RESP {
	if len(args) == 0 {
		return &RESP{Type: STRING, Value: "PONG"}
	}
	return &RESP{Type: STRING, Value: args[0].Value}
}

func echo(args []*RESP) *RESP {
	if len(args) == 0 {
		return &RESP{Type: STRING, Value: ""}
	}
	return &RESP{Type: STRING, Value: args[0].Value}
}

func info(args []*RESP) *RESP {
	if len(args) != 1 {
		return NullResp()
	}
	switch args[0].Value {
	case "replication":
		return &RESP{
			Type: BULK,
			// Value: "role:" + ThisServer.Type.String(),
			Value: "# Replication\n" +
				"role:" + ThisServer.Role.String() + "\n" +
				"master_replid:" + ThisServer.MasterReplid + "\n" +
				"master_repl_offset:" + strconv.Itoa(ThisServer.MasterReplOffset) + "\n",
		}
	default:
		return NullResp()
	}
}

// TODO
func replConfig(args []*RESP) (*RESP, bool) {
	if len(args) != 2 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'replconf' command"}, false
	}
	if strings.ToUpper(args[0].Value) == "GETACK" && args[1].Value == "*" {
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "ACK"},
				{Type: BULK, Value: "0"},
			},
		}, true
	}
	return OkResp(), false
}

func RequestAck() *RESP {
	return &RESP{
		Type: ARRAY,
		Values: []*RESP{
			{Type: BULK, Value: "replconf"},
			{Type: BULK, Value: "getack"},
			{Type: BULK, Value: "*"},
		},
	}
}

const EmptyRBD = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

func getRDB() *RESP {
	return &RESP{
		Type:  RDB,
		Value: EmptyRBD,
	}
}

func psync() *RESP {
	return &RESP{
		Type:  STRING,
		Value: "FULLRESYNC " + ThisServer.MasterReplid + " " + strconv.Itoa(ThisServer.MasterReplOffset),
	}
}

// ----------------------------------------------------------------------------

// Get and Set function and storage -------------------------------------------

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []*RESP) *RESP {
	if !(len(args) == 2 || len(args) == 4) {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'set' command"}
	}

	var length int
	if len(args) > 2 {
		if strings.ToLower(args[2].Value) != "px" {
			return &RESP{Type: ERROR, Value: "ERR syntax error"}
		}

		l, err := strconv.Atoi(args[3].Value)
		if err != nil {
			return &RESP{Type: ERROR, Value: "ERR value is not an integer or out of range"}
		}
		length = l
	}

	key, value := args[0].Value, args[1].Value

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()
	if length > 0 {
		time.AfterFunc(time.Duration(length)*time.Millisecond, func() {
			SETsMu.Lock()
			delete(SETs, key)
			SETsMu.Unlock()
		})
	}

	return OkResp()
}

func get(args []*RESP) *RESP {
	if len(args) != 1 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Value

	SETsMu.Lock()
	value, ok := SETs[key]
	SETsMu.Unlock()

	if !ok {
		return NullResp()
	}

	return &RESP{Type: STRING, Value: value}
}

// ----------------------------------------------------------------------------

func checkOnReplica(w *Writer) {
	for {
		fmt.Println("Checking on replica")
		time.Sleep(5 * time.Second)
		w.Write(RequestAck())
	}
}

var CommandList = map[string]struct{}{
	"PING": {}, "ECHO": {}, "SET": {}, "GET": {}, "INFO": {}, "REPLCONF": {}, "PSYNC": {}, "COMMAND": {},
}
var WriteCommands = map[string]struct{}{
	"SET": {}, "INFO": {},
}
var GetCommands = map[string]struct{}{
	"PING": {}, "ECHO": {}, "GET": {}, "REPLCONF": {}, "PSYNC": {}, "COMMAND": {},
}

func (w *Writer) handleArray(resp *RESP) []*RESP {
	command := strings.ToUpper(resp.Values[0].Value)
	args := resp.Values[1:]
	switch command {
	case "PING":
		return []*RESP{ping(args)}
	case "ECHO":
		return []*RESP{echo(args)}
	case "SET":
		propagateCommand(resp)
		return []*RESP{set(args)}
	case "GET":
		return []*RESP{get(args)}
	case "INFO":
		return []*RESP{info(args)}
	case "REPLCONF":
		resp, toWrite := replConfig(args)
		if toWrite {
			fmt.Println("Writing:", resp)
			w.Write(resp)
		}
		return []*RESP{resp}
	case "PSYNC":
		ThisServer.Writers = append(ThisServer.Writers, w)
		// defer func() {
		// 	go checkOnReplica(w)
		// }()
		return []*RESP{psync(), getRDB()}
	case "COMMAND":
		return []*RESP{commandFunc()}
	default:
		return []*RESP{{Type: ERROR, Value: "Unknown command " + command}}
	}
}

func propagateCommand(resp *RESP) {
	for _, w := range ThisServer.Writers {
		w.Write(resp)
	}
}

func (w *Writer) Handler(response *RESP) (resp []*RESP) {
	switch response.Type {
	case ERROR, INTEGER, BULK, STRING:
		return []*RESP{{Type: ERROR, Value: "Response type " + response.Value + " handle not yet implemented"}}
	case ARRAY:
		return w.handleArray(response)
	default:
		return []*RESP{{Type: ERROR, Value: "Response type " + response.Value + " not recognized"}}
	}
}
