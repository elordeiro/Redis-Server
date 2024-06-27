package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Handler entry point --------------------------------------------------------
func (s *Server) Handler(parsedResp *RESP, conn *ConnRW) (resp []*RESP) {
	switch parsedResp.Type {
	case ERROR, INTEGER, BULK, STRING:
		return []*RESP{{Type: ERROR, Value: "Response type " + parsedResp.Value + " handle not yet implemented"}}
	case ARRAY:
		return s.handleArray(parsedResp, conn)
	default:
		return []*RESP{{Type: ERROR, Value: "Response type " + parsedResp.Value + " not recognized"}}
	}
}

func (s *Server) handleArray(resp *RESP, conn *ConnRW) []*RESP {
	command, args := resp.getCmdAndArgs()
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
		s.replConfig(args, conn)
		return []*RESP{}
	case "PSYNC":
		conn.Type = REPLICA
		s.ReplicaCount++
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
	for _, conn := range s.Conns {
		if conn.Type != REPLICA {
			continue
		}
		marshaled := resp.Marshal()
		s.MasterReplOffset += len(marshaled)
		Write(conn.Writer, marshaled)
	}
}

func (s *Server) checkOnReplica(w *Writer) {
	getAckResp := GetAckResp().Marshal()
	n := len(getAckResp)
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Checking On Replica")
		s.MasterReplOffset += n
		Write(w, getAckResp)
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
			{Type: BULK, Value: "REPLCONF"},
			{Type: BULK, Value: "GETACK"},
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

// ----------------------------------------------------------------------------

// Server specific commands ---------------------------------------------------
func (s *Server) set(args []*RESP) *RESP {
	if !(len(args) == 2 || len(args) == 4) {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'set' command"}
	}
	s.NeedAcks = true
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

func (s *Server) replConfig(args []*RESP, conn *ConnRW) (resp *RESP) {
	if len(args) != 2 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'replconf' command"}
	}

	if strings.ToUpper(args[0].Value) == "GETACK" && args[1].Value == "*" {
		// Replica recieved REPLCONF GETACK * -> Send ACK <offset> to master
		resp = &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: BULK, Value: "REPLCONF"},
				{Type: BULK, Value: "ACK"},
				{Type: BULK, Value: strconv.Itoa(s.MasterReplOffset)},
			},
		}
		fmt.Println("Response: ", resp)
		Write(conn.Writer, resp)
	} else if strings.ToUpper(args[0].Value) == "ACK" {
		// Master recieved REPLCONF ACK <offset> from replica -> Read <offset> from replica
		resp = &RESP{
			Type:  INTEGER,
			Value: args[1].Value,
		}
	} else {
		// Master recieved REPLCONF listening-port <port> or REPLCONF capa psync2 from replica -> Do nothing
		resp = OkResp()
		Write(conn.Writer, resp)
	}
	return resp
}

func (s *Server) sendGetAckCommand(getAck []byte) {
	for _, c := range s.Conns {
		if c.Type != REPLICA {
			continue
		}
		Write(c.Writer, getAck)
	}
}

func (s *Server) wait(args []*RESP) *RESP {
	if !s.NeedAcks {
		return &RESP{Type: INTEGER, Value: strconv.Itoa(s.ReplicaCount)}
	}
	getAck := GetAckResp().Marshal()
	defer func() {
		s.MasterReplOffset += len(getAck)
		s.Redirect = false
		s.NeedAcks = false
		fmt.Println("")
	}()

	numReplicas, _ := strconv.Atoi(args[0].Value)
	timeout, _ := strconv.Atoi(args[1].Value)

	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	acks := 0

	s.Redirect = true
	go s.sendGetAckCommand(getAck)

	for {
		select {
		case <-timeoutChan:
			return &RESP{
				Type:  INTEGER,
				Value: strconv.Itoa(acks),
			}
		default:
			for _, c := range s.Conns {
				if c.Type != REPLICA {
					continue
				}
				select {
				case parsedResp := <-c.Chan:
					fmt.Println("Received ACK from replica")
					_, args := parsedResp.getCmdAndArgs()
					result := s.replConfig(args, c)
					strconv.Atoi(result.Value)
					// replOffset, _ := strconv.Atoi(result.Value)
					// if replOffset == s.MasterReplOffset {
					acks++
					if acks == numReplicas {
						return &RESP{
							Type:  INTEGER,
							Value: strconv.Itoa(acks),
						}
					}
					// }
				case <-timeoutChan:
					return &RESP{
						Type:  INTEGER,
						Value: strconv.Itoa(acks),
					}
				default:
					continue
				}
			}
		}
	}
}

// ----------------------------------------------------------------------------
