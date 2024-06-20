package main

import (
	"strings"
	"sync"
)

func okResp() *RESP {
	return &RESP{Type: STRING, Value: "OK"}
}

func nullResp() *RESP {
	return &RESP{Type: NULL}
}

func commandFunc(args []*RESP) *RESP {
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

// Get and Set function and storage -------------------------------------------

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []*RESP) *RESP {
	if len(args) != 2 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'set' command"}
	}

	key, value := args[0].Value, args[1].Value

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return okResp()
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
		return nullResp()
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
	case "COMMAND":
		return commandFunc(args)
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
