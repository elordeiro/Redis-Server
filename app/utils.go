package main

import (
	"bufio"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/radix"
	"golang.org/x/exp/constraints"
)

// RESP related ---------------------------------------------------------------
func (resp *RESP) String() string {
	var str string
	switch resp.Type {
	case ARRAY:
		str = "[ "
		for i := range resp.Values {
			str += resp.Values[i].String() + " "
		}
		str += "]"
	case BULK, STRING, ERROR, INTEGER, RDB:
		str = resp.Value
	default:
		return ""
	}
	str += " "
	return str
}

func (resp *RESP) getCmdAndArgs() (string, []*RESP) {
	command := strings.ToUpper(resp.Values[0].Value)
	args := resp.Values[1:]
	return command, args
}

func ToRespArray(values []string) []*RESP {
	resps := make([]*RESP, len(values))
	for i := range values {
		resps[i] = &RESP{
			Type:  STRING,
			Value: values[i],
		}
	}
	return resps
}

// ----------------------------------------------------------------------------

// Predefined responses -------------------------------------------------------
func OkResp() *RESP {
	return &RESP{Type: STRING, Value: "OK"}
}

func NullResp() *RESP {
	return &RESP{Type: NULL}
}

func ErrResp(err string) *RESP {
	return &RESP{Type: ERROR, Value: err}
}

func QueuedResp() *RESP {
	return &RESP{
		Type:  STRING,
		Value: "QUEUED",
	}
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

func SimpleString(s string) *RESP {
	return &RESP{
		Type:  STRING,
		Value: s,
	}
}

func BulkString(s string) *RESP {
	return &RESP{
		Type:  BULK,
		Value: s,
	}
}

func Integer[T constraints.Signed](i T) *RESP {
	return &RESP{
		Type:  INTEGER,
		Value: intToStr(i),
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

func (resp *RESP) IsExec() bool {
	if resp.Type != ARRAY {
		return false
	}
	if strings.ToUpper(resp.Values[0].Value) != "EXEC" {
		return false
	}
	return true
}

func (resp *RESP) IsDiscard() bool {
	if resp.Type != ARRAY {
		return false
	}
	if strings.ToUpper(resp.Values[0].Value) != "DISCARD" {
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

// Decode RDB helpers --------------------------------------------------------
func decodeSize(r *bufio.Reader) (int, error) {
	bt, _ := r.ReadByte()
	switch bt >> 6 {
	case 0:
		return int(bt), nil
	case 1:
		next, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(bt&0x3F)<<8 | int(next), nil
	case 2:
		next4 := make([]byte, 4)
		_, err := io.ReadFull(r, next4)
		if err != nil {
			return 0, err
		}
		return int(next4[0])<<24 | int(next4[1])<<16 | int(next4[2])<<8 | int(next4[3]), nil
	default:
		return 0, errors.New("error decoding size bytes")
	}
}

func decodeString(r *bufio.Reader) (string, error) {
	bt, _ := r.ReadByte()
	switch {
	case bt < 0xc0:
		str := make([]byte, int(bt))
		io.ReadFull(r, str)
		return string(str), nil
	case bt == 0xC0:
		next, _ := r.ReadByte()
		return strconv.Itoa(int(next)), nil
	case bt == 0xC1:
		next2 := make([]byte, 2)
		_, err := io.ReadFull(r, next2)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(next2[1])<<8 | int(next2[0])), nil
	case bt == 0xC2:
		next4 := make([]byte, 4)
		_, err := io.ReadFull(r, next4)
		if err != nil {
			return "", err
		}
		return strconv.Itoa(int(next4[3])<<24 | int(next4[2])<<16 | int(next4[1])<<8 | int(next4[0])), nil
	case bt == 0xC3:
		return "", errors.New("LZF compression not supported")
	default:
		return "", errors.New("error decoding string")
	}
}

func dedodeTime(r *bufio.Reader) (int64, error) {
	byt, _ := r.ReadByte()
	var expiryTime int64 = 0
	if byt == 0xfc {
		expiry := make([]byte, 8)
		_, err := io.ReadFull(r, expiry)
		if err != nil {
			return 0, err
		}
		expiryTime =
			int64(expiry[7])<<56 | int64(expiry[6])<<48 | int64(expiry[5])<<40 | int64(expiry[4])<<32 |
				int64(expiry[3])<<24 | int64(expiry[2])<<16 | int64(expiry[1])<<8 | int64(expiry[0])
	} else if byt == 0xfd {
		expiry := make([]byte, 4)
		_, err := io.ReadFull(r, expiry)
		if err != nil {
			return 0, err
		}
		expiryTime = int64(expiry[3])<<24 | int64(expiry[2])<<16 | int64(expiry[1])<<8 | int64(expiry[0])
	} else {
		r.UnreadByte()
		return 0, nil
	}
	return expiryTime, nil
}

// ----------------------------------------------------------------------------

// Stream helpers ------------------------------------------------------------
func GetTopEntry(stream *radix.Radix) (int64, int64) {
	top, _ := stream.Find("0-0")
	return top.(*StreamTop).Time, top.(*StreamTop).Seq
}

func validateEntryID(stream *radix.Radix, key string) (int64, int64, error) {
	if key == "*" {
		time := time.Now().UnixMilli()
		topTime, topSeq := GetTopEntry(stream)
		var seq int64
		if time == topTime {
			seq = topSeq + 1
		} else {
			seq = 0
		}
		return time, seq, nil
	}

	if key == "0-0" {
		return 0, 0, errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	parts := strings.Split(key, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid stream key")
	}

	topTime, topSeq := GetTopEntry(stream)
	time, _ := strconv.ParseInt(parts[0], 10, 64)
	if time < topTime {
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	var seq int64
	if parts[1] == "*" {
		if time == topTime {
			seq = topSeq + 1
		} else {
			seq = 0
		}
	} else {
		seq, _ = strconv.ParseInt(parts[1], 10, 64)
	}

	if time == topTime && seq <= topSeq {
		return 0, 0, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return time, seq, nil
}

func splitEntryId(id string) (int64, int64, error) {
	if id == "-" {
		return math.MinInt64, math.MinInt64, nil
	}
	if id == "+" {
		return math.MaxInt64, math.MaxInt64, nil
	}
	parts := strings.Split(id, "-")
	time, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if len(parts) == 1 {
		return time, 0, nil
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return time, seq, nil
}

func intToStr[T constraints.Signed](num T) string {
	return strconv.Itoa(int(num))
}

// ----------------------------------------------------------------------------
