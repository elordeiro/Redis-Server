package main

import (
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
func replConfig() *RESP {
	return OkResp()
}

// TODO
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

func handleArray(arr []*RESP) *RESP {
	command := strings.ToUpper(arr[0].Value)
	args := arr[1:]
	switch command {
	case "PING":
		return ping(args)
	case "ECHO":
		return echo(args)
	case "SET":
		return set(args)
	case "GET":
		return get(args)
	case "INFO":
		return info(args)
	case "REPLCONF":
		return replConfig()
	case "PSYNC":
		return psync()
	case "COMMAND":
		return commandFunc()
	default:
		return &RESP{Type: ERROR, Value: "Unknown command " + command}
	}
}

func Handler(response *RESP) *RESP {
	switch response.Type {
	case ERROR, INTEGER, BULK, STRING:
		return &RESP{Type: ERROR, Value: "Response type " + response.Value + " handle not yet implemented"}
	case ARRAY:
		return handleArray(response.Values)
	default:
		return &RESP{Type: ERROR, Value: "Response type " + response.Value + " not recognized"}
	}
}
