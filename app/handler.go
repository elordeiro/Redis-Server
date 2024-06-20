package main

import (
	"strings"
)

func commandFunc(args []*RESP) *RESP {
	return &RESP{Type: '_', Value: "Command"}
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

func handleArray(arr []*RESP) *RESP {
	command := strings.ToUpper(arr[0].Value)
	args := arr[1:]
	switch command {
	case "PING":
		return ping(args)
	case "ECHO":
		return echo(args)
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
