package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Handler entry point --------------------------------------------------------
func (s *Server) Handler(parsedResp *RESP, w *Writer) (resp []*RESP) {
	switch parsedResp.Type {
	case ERROR, INTEGER, BULK, STRING:
		return []*RESP{{Type: ERROR, Value: "Response type " + parsedResp.Value + " handle not yet implemented"}}
	case ARRAY:
		return s.handleArray(parsedResp, w)
	default:
		return []*RESP{{Type: ERROR, Value: "Response type " + parsedResp.Value + " not recognized"}}
	}
}

func (s *Server) handleArray(resp *RESP, w *Writer) []*RESP {
	command := strings.ToUpper(resp.Values[0].Value)
	args := resp.Values[1:]
	switch command {
	case "PING":
		return []*RESP{ping(args)}
	case "ECHO":
		return []*RESP{echo(args)}
	case "SET":
		s.propagateCommand(resp)
		return []*RESP{s.set(args)}
	case "GET":
		return []*RESP{s.get(args)}
	case "INFO":
		return []*RESP{info(args, s.Role.String(), s.MasterReplid, s.MasterReplOffset)}
	case "REPLCONF":
		resp := replConfig(args, s.MasterReplOffset)
		if s.Role == REPLICA {
			fmt.Println("Response: ", resp)
			w.Write(resp)
		}
		if resp == nil {
			return []*RESP{}
		}
		return []*RESP{resp}
	case "PSYNC":
		s.Writers = append(s.Writers, w)
		defer func() {
			// go s.checkOnReplica(w)
		}()
		return []*RESP{psync(s.MasterReplid, s.MasterReplOffset), getRDB()}
	case "WAIT":
		return []*RESP{s.wait(args)}
	case "COMMAND":
		return []*RESP{commandFunc()}
	default:
		return []*RESP{{Type: ERROR, Value: "Unknown command " + command}}
	}
}

func (s *Server) propagateCommand(resp *RESP) {
	for _, w := range s.Writers {
		s.MasterReplOffset += resp.Len()
		w.Write(resp)
	}
}

func (s *Server) checkOnReplica(w *Writer) {
	getAckResp := GetAckResp()
	n := getAckResp.Len()
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Checking On Replica")
		s.MasterReplOffset += n
		w.Write(getAckResp)
		// time.Sleep(120 * time.Second)
	}
}

// ----------------------------------------------------------------------------

// Predefined responses -------------------------------------------------------
func OkResp() *RESP {
	return &RESP{Type: STRING, Value: "OK"}
}

func NullResp() *RESP {
	return &RESP{Type: NULL}
}

func GetAckResp() *RESP {
	return &RESP{
		Type: ARRAY,
		Values: []*RESP{
			{Type: BULK, Value: "replconf"},
			{Type: BULK, Value: "getack"},
			{Type: BULK, Value: "*"},
		},
	}
}

// ----------------------------------------------------------------------------

// Handshake helpers ----------------------------------------------------------
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
func ReplconfResp(i int, port string) *RESP {
	switch i {
	case 1:
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "listening-port"},
				{Type: BULK, Value: port},
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

// Can be used for handshake stage 3 as Replica
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

// Can be used for handshake stage 3 as Master
const EmptyRBD = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

func getRDB() *RESP {
	return &RESP{
		Type:  RDB,
		Value: EmptyRBD,
	}
}

func psync(mrid string, mros int) *RESP {
	return &RESP{
		Type:  STRING,
		Value: "FULLRESYNC " + mrid + " " + strconv.Itoa(mros),
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

// General commands -----------------------------------------------------------
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

func info(args []*RESP, role, mrid string, mros int) *RESP {
	if len(args) != 1 {
		return NullResp()
	}
	switch args[0].Value {
	case "replication":
		return &RESP{
			Type: BULK,
			Value: "# Replication\n" +
				"role:" + role + "\n" +
				"master_replid:" + mrid + "\n" +
				"master_repl_offset:" + strconv.Itoa(mros) + "\n",
		}
	default:
		return NullResp()
	}
}

func replConfig(args []*RESP, mros int) *RESP {
	if len(args) != 2 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'replconf' command"}
	}
	if strings.ToUpper(args[0].Value) == "GETACK" && args[1].Value == "*" {
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "ACK"},
				{Type: BULK, Value: strconv.Itoa(mros)},
			},
		}
	}
	if strings.ToUpper(args[0].Value) == "ACK" {
		return nil
	}
	return OkResp()
}

// ----------------------------------------------------------------------------

// Server specific commands ---------------------------------------------------

func (s *Server) set(args []*RESP) *RESP {
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

	s.SETsMu.Lock()
	s.SETs[key] = value
	s.SETsMu.Unlock()
	if length > 0 {
		time.AfterFunc(time.Duration(length)*time.Millisecond, func() {
			s.SETsMu.Lock()
			delete(s.SETs, key)
			s.SETsMu.Unlock()
		})
	}

	return OkResp()
}

func (s *Server) get(args []*RESP) *RESP {
	if len(args) != 1 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Value

	s.SETsMu.Lock()
	value, ok := s.SETs[key]
	s.SETsMu.Unlock()

	if !ok {
		return NullResp()
	}

	return &RESP{Type: STRING, Value: value}
}

func (s *Server) wait(args []*RESP) *RESP {
	numReplicas, _ := strconv.Atoi(args[0].Value)
	timeout, _ := strconv.Atoi(args[1].Value)

	done := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		done <- true
	}()

	for i, w := range s.Writers {
		if <-done {
			return &RESP{
				Type:  INTEGER,
				Value: strconv.Itoa(len(s.Writers)),
			}
		}
		if i == numReplicas {
			break
		}
		w.Write(GetAckResp())
	}

	return &RESP{
		Type:  INTEGER,
		Value: strconv.Itoa(len(s.Writers)),
	}
}

// ----------------------------------------------------------------------------
