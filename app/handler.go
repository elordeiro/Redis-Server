package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radix"
)

// Handler entry point --------------------------------------------------------
func (s *Server) Handler(parsedResp *RESP, conn *ConnRW) (resp []*RESP) {
	switch parsedResp.Type {
	case ERROR, INTEGER, BULK, STRING:
		return []*RESP{{Type: ERROR, Value: "Response type " + parsedResp.Value + " handle not yet implemented"}}
	case ARRAY:
		return s.handleArray(parsedResp, conn)
	case RDB:
		return []*RESP{s.decodeRDB(NewBuffer(bytes.NewReader([]byte(parsedResp.Value))))}
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
	case "XADD":
		return []*RESP{s.xadd(args)}
	case "XRANGE":
		return []*RESP{s.xrange(args)}
	case "XREAD":
		go func() {
			result := s.xread(args)
			Write(conn.Writer, result)
		}()
		return []*RESP{}
	case "INCR":
		return []*RESP{s.incr(args)}
	case "INFO":
		return []*RESP{info(args, s.Role.String(), s.MasterReplid, s.MasterReplOffset)}
	case "REPLCONF":
		s.replConfig(args, conn)
		return []*RESP{}
	case "PSYNC":
		conn.Type = REPLICA
		s.ReplicaCount++
		go s.checkOnReplica(conn, false)
		return []*RESP{psync(s.MasterReplid, s.MasterReplOffset), getRDB()}
	case "WAIT":
		return []*RESP{s.wait(args)}
	case "KEYS":
		return []*RESP{s.keys(args)}
	case "TYPE":
		return []*RESP{s.typecmd(args)}
	case "MULTI":
		go func() {
			s.multi(conn)
		}()
		return []*RESP{OkResp()}
	case "EXEC":
		return []*RESP{s.exec(conn)}
	case "DISCARD":
		return []*RESP{s.discard()}
	case "CONFIG":
		return []*RESP{s.config(args)}
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

func (s *Server) checkOnReplica(conn *ConnRW, featureOn bool) {
	if !featureOn {
		return
	}
	getAckResp := GetAckResp().Marshal()
	n := len(getAckResp)
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Checking On Replica")
		s.MasterReplOffset += n
		Write(conn.Writer, getAckResp)
	}
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
func (s *Server) decodeRDB(buf *Buffer) *RESP {
	data := buf.reader

	// Header section
	header := make([]byte, 9)
	_, err := io.ReadFull(data, header)
	if err != nil {
		return ErrResp("Error reading RDB header")
	}

	if string(header[:5]) != "REDIS" {
		return ErrResp("Invalid RDB file")
	}

	// version := string(header[5:])
	// if version != "0007" {
	// 	return ErrResp("Invalid RDB version")
	// }

	// Metadata section
	for {
		fa, err := data.ReadByte()
		if err != nil {
			return ErrResp("Error reading metadata section")
		}
		if fa != 0xfa {
			data.UnreadByte()
			break
		}

		// Metadataa Key
		_, err = decodeString(data)
		if err != nil {
			return ErrResp("Error reading metadata section")
		}
		// Metadata Value
		_, err = decodeString(data)
		if err != nil {
			return ErrResp("Error reading metadata section")
		}
	}

	for {
		byt, _ := data.Peek(1)
		if byt[0] == 0xff {
			break
		}
		// Database section - 0xfe
		data.ReadByte()

		// This byte is the database index
		// TODO - Implement support for multiple databases
		decodeSize(data)

		fb, err := data.ReadByte()
		if err != nil || fb != 0xfb {
			return ErrResp("Error reading database section")
		}

		dbsize, err := decodeSize(data)
		if err != nil {
			return ErrResp("Error reading database section")
		}

		// Expiry size
		_, err = decodeSize(data)
		if err != nil {
			return ErrResp("Error reading database section")
		}

		// Iterate over keys
		for i := 0; i < dbsize; i++ {
			// Expiry
			expiryTime, err := dedodeTime(data)
			if err != nil {
				return ErrResp("Error reading expiry")
			}

			// This byte is the key type
			// TODO - Implement support for different key types
			data.ReadByte()

			// Key
			key, err := decodeString(data)
			if err != nil {
				return ErrResp("Error reading key")
			}

			// Value
			value, err := decodeString(data)
			if err != nil {
				return ErrResp("Error reading value")
			}

			s.SETsMu.Lock()
			s.SETs[string(key)] = string(value)
			if expiryTime > 0 {
				s.EXPs[string(key)] = expiryTime
				fmt.Println("Key: ", key, "Value: ", value, "Expiry: ", expiryTime)
			}
			s.SETsMu.Unlock()
		}

		next, _ := data.Peek(1)
		if next[0] == 0xff {
			break
		}
	}
	return OkResp()
}

func (s *Server) keys(args []*RESP) *RESP {
	if len(args) != 1 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'keys' command"}
	}

	pattern := args[0].Value
	keys := []string{}

	if pattern == "*" {
		s.SETsMu.Lock()
		for k := range s.SETs {
			keys = append(keys, k)
		}
		s.SETsMu.Unlock()
	} else {
		s.SETsMu.Lock()
		for k := range s.SETs {
			if strings.Contains(k, pattern) {
				keys = append(keys, k)
			}
		}
		s.SETsMu.Unlock()
	}

	return &RESP{
		Type:   ARRAY,
		Values: ToRespArray(keys),
	}
}

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
	if length > 0 {
		// Set expiry time in milliseconds
		s.EXPs[key] = time.Now().Add(time.Duration(length) * time.Millisecond).UnixMilli()
	}
	s.SETsMu.Unlock()

	return OkResp()
}

func (s *Server) get(args []*RESP) *RESP {
	if len(args) != 1 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].Value

	s.SETsMu.Lock()
	value, ok := s.SETs[key]
	if exp, ok := s.EXPs[key]; ok {
		expTime := time.UnixMilli(exp)
		if time.Now().After(expTime) {
			delete(s.SETs, key)
			delete(s.EXPs, key)
			s.SETsMu.Unlock()
			return NullResp()
		}
	}
	s.SETsMu.Unlock()

	if !ok {
		return NullResp()
	}

	return &RESP{Type: STRING, Value: value}
}

func (s *Server) xadd(args []*RESP) *RESP {
	if len(args) < 2 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'xadd' command"}
	}

	streamKey := args[0].Value
	stream, ok := s.XADDs[streamKey]
	if !ok {
		s.XADDsMu.Lock()
		stream = radix.NewRadix()
		s.XADDs[streamKey] = stream
		stream.Insert("0-0", &StreamTop{Time: 0, Seq: 0})
		s.XADDsMu.Unlock()
	}

	id := args[1].Value
	time, seq, err := validateEntryID(stream, id)
	if err != nil {
		return ErrResp(err.Error())
	}

	entries := []*StreamKV{}
	for i := 2; i < len(args); i += 2 {
		entries = append(entries, &StreamKV{Key: args[i].Value, Value: args[i+1].Value})
	}

	timeStr := intToStr(time) + "-" + intToStr(seq)
	streamEntry := &StreamEntry{Seq: seq, Entries: entries}
	stream.Insert(timeStr, streamEntry)
	stream.Insert("0-0", &StreamTop{Time: time, Seq: seq})

	if s.XREADsBlock {
		s.XADDsCh <- false
	}

	return &RESP{Type: BULK, Value: timeStr}
}

func (s *Server) xrange(args []*RESP) *RESP {
	if len(args) < 3 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'xrange' command"}
	}

	streamKey := args[0].Value
	stream, ok := s.XADDs[streamKey]
	if !ok {
		return ErrResp("ERR stream not found")
	}

	// st = starttime, ss = startseq
	st, ss, err := splitEntryId(args[1].Value)
	if err != nil {
		return ErrResp(err.Error())
	}
	if st == math.MinInt64 {
		key, _, _ := stream.GetFirst()
		st, ss, _ = splitEntryId(key)
	}
	// et = endtime, es = endseq
	et, es, err := splitEntryId(args[2].Value)
	if err != nil {
		return ErrResp(err.Error())
	}
	if et == math.MaxInt64 {
		key, _, _ := stream.GetLast()
		et, es, _ = splitEntryId(key)
	}

	entries := []*RESP{}

	for t := st; t <= et; t++ {
		tStr := intToStr(t)
		sEntries := stream.FindAll(tStr)
		for _, e := range sEntries {
			switch entry := e.(type) {
			case *StreamEntry:
				if t > st || t < et ||
					(t == st && entry.Seq >= ss && t == et && entry.Seq <= es) {
					outter := []*RESP{}
					outter = append(outter, SimpleString(tStr+"-"+intToStr(entry.Seq)))
					inner := make([]string, 0, len(entry.Entries)*2)
					for _, en := range entry.Entries {
						inner = append(inner, en.Key)
						inner = append(inner, en.Value)
					}
					outter = append(outter, &RESP{Type: ARRAY, Values: ToRespArray(inner)})
					entries = append(entries, &RESP{Type: ARRAY, Values: outter})
				}
			default:
				continue
			}
		}
	}

	return &RESP{Type: ARRAY, Values: entries}
}

func (s *Server) xread(args []*RESP) *RESP {
	if len(args) < 3 {
		return &RESP{Type: ERROR, Value: "ERR wrong number of arguments for 'xread' command"}
	}

	blockTime := -1
	if strings.ToUpper(args[0].Value) == "BLOCK" {
		t, err := strconv.Atoi(args[1].Value)
		if err != nil {
			return ErrResp("ERR block time is not an integer or out of range")
		}
		blockTime = t
		args = args[2:]
	}

	if blockTime > 0 {
		time.Sleep(time.Duration(blockTime) * time.Millisecond)
	} else if blockTime == 0 {
		s.XREADsBlock = true
		s.XREADsBlock = <-s.XADDsCh
	}

	if args[0].Value != "streams" {
		return &RESP{Type: ERROR, Value: "ERR can only read streams at the moment"}
	}

	args = args[1:]
	if len(args)%2 != 0 {
		return ErrResp("Err wrong number of arguments for 'xread' command")
	}

	readLen := len(args) / 2

	streamLst := []*RESP{}

	for i := 0; i < readLen; i++ {
		streamKey := args[i].Value
		stream, ok := s.XADDs[streamKey]
		if !ok {
			return ErrResp("ERR stream not found")
		}

		start := args[i+readLen].Value
		if start == "$" {
			start, _, _ = stream.GetLast()
		} else {
			start, _, ok = stream.GetNext(start)
			if !ok {
				return NullResp()
			}
		}
		// st = starttime, ss = startseq
		st, ss, err := splitEntryId(start)
		if err != nil {
			return ErrResp(err.Error())
		}

		// et == endtime, es = endseq
		last, _, _ := stream.GetLast()
		et, es, _ := splitEntryId(last)

		entryLst := []*RESP{BulkString(streamKey)}

		for t := st; t <= et; t++ {
			tLst := []*RESP{}
			tStr := intToStr(t)
			sEntries := stream.FindAll(tStr)
			for _, e := range sEntries {
				switch entry := e.(type) {
				case *StreamEntry:
					if t > st || t < et ||
						(t == st && entry.Seq >= ss && t == et && entry.Seq <= es) {
						idLst := []*RESP{BulkString(tStr + "-" + intToStr(entry.Seq))}
						kvLst := make([]string, 0, len(entry.Entries)*2)
						for _, en := range entry.Entries {
							kvLst = append(kvLst, en.Key)
							kvLst = append(kvLst, en.Value)
						}
						idLst = append(idLst, &RESP{Type: ARRAY, Values: ToRespArray(kvLst)})
						tLst = append(tLst, &RESP{Type: ARRAY, Values: idLst})
					}
				default:
					continue
				}
			}
			entryLst = append(entryLst, &RESP{Type: ARRAY, Values: tLst})
		}
		streamLst = append(streamLst, &RESP{Type: ARRAY, Values: entryLst})
	}

	return &RESP{Type: ARRAY, Values: streamLst}
}

func (s *Server) incr(args []*RESP) *RESP {
	if len(args) != 1 {
		return ErrResp("ERR wrong number of arguments for 'incr' command")
	}
	key := args[0].Value
	s.SETsMu.Lock()
	defer s.SETsMu.Unlock()
	if val, ok := s.SETs[key]; ok {
		val, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return ErrResp("ERR value is not an integer or out of range")
		}
		s.SETs[key] = intToStr(val + 1)
		return Integer(val + 1)
	} else {
		s.SETs[key] = "1"
		return Integer(1)
	}
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

func (s *Server) wait(args []*RESP) *RESP {
	if !s.NeedAcks {
		return &RESP{Type: INTEGER, Value: strconv.Itoa(s.ReplicaCount)}
	}
	getAck := GetAckResp().Marshal()
	defer func() {
		s.MasterReplOffset += len(getAck)
		s.RedirectRead = false
		s.NeedAcks = false
		fmt.Println("")
	}()

	numReplicas, _ := strconv.Atoi(args[0].Value)
	timeout, _ := strconv.Atoi(args[1].Value)

	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	acks := 0

	s.RedirectRead = true
	go func() {
		for _, c := range s.Conns {
			if c.Type != REPLICA {
				continue
			}
			Write(c.Writer, getAck)
		}
	}()

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

func (s *Server) multi(conn *ConnRW) {
	conn.RedirectRead = true
	q := s.MultiProps.Queue
	for {
		resp := <-conn.Chan
		if resp.IsExec() {
			break
		}
		if resp.IsDiscard() {
			q.Clear()
			Write(conn.Writer, OkResp())
			conn.RedirectRead = false
			return
		}
		q.Enqueue(resp)
		Write(conn.Writer, QueuedResp())
	}
	s.exec(conn)
}

func (s *Server) exec(conn *ConnRW) *RESP {
	if !conn.RedirectRead {
		return ErrResp("ERR EXEC without MULTI")
	}
	q := s.MultiProps.Queue
	response := &RESP{Type: ARRAY, Values: []*RESP{}}

	for !q.IsEmpty() {
		resp, _ := q.Dequeue()
		results := s.Handler(resp.(*RESP), conn)
		response.Values = append(response.Values, results...)
	}

	Write(conn.Writer, response)
	conn.RedirectRead = false

	return OkResp()
}

func (s *Server) discard() *RESP {
	return ErrResp("ERR DISCARD without MULTI")
}

func (s *Server) config(args []*RESP) *RESP {
	if strings.ToUpper(args[0].Value) == "GET" {
		if strings.ToLower(args[1].Value) == "dir" {
			return &RESP{
				Type: ARRAY,
				Values: []*RESP{
					{Type: STRING, Value: "dir"},
					{Type: STRING, Value: s.Dir},
				},
			}
		}
		return &RESP{
			Type: ARRAY,
			Values: []*RESP{
				{Type: STRING, Value: "dbfilename"},
				{Type: STRING, Value: s.Dbfilename},
			},
		}
	}
	return &RESP{
		Type:  ERROR,
		Value: "ERR unknown subcommand or wrong number of arguments",
	}
}

func (s *Server) typecmd(args []*RESP) *RESP {
	if len(args) == 0 {
		return ErrResp("Err no key given to TYPE command")
	}
	if len(args) > 1 {
		return ErrResp("Too many keys given to TYPE command")
	}

	key := args[0].Value

	s.SETsMu.Lock()
	_, ok := s.SETs[key]
	s.SETsMu.Unlock()
	if ok {
		return SimpleString("string")
	}

	s.XADDsMu.Lock()
	_, ok = s.XADDs[key]
	s.XADDsMu.Unlock()
	if ok {
		return SimpleString("stream")
	}

	return SimpleString("none")
}

// ----------------------------------------------------------------------------
