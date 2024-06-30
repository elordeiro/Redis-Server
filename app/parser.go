package main

import (
	"errors"

	"io"
	"strconv"
	"strings"
)

// Deserialize ----------------------------------------------------------------
func (buf *Buffer) Read() (*RESP, int, error) {
	typ, err := buf.reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}

	var resp *RESP
	var n int
	switch typ {
	case ARRAY:
		resp, n, err = buf.readArray()
	case BULK:
		resp, n, err = buf.readBulkString()
	case STRING:
		resp, n, err = buf.readString()
	case ERROR:
		return nil, 0, nil
	case INTEGER:
		resp, n, err = buf.readInteger()
	default:
		return nil, 0, errors.New("invalid type")
	}
	return resp, 1 + n, err
}

func (buf *Buffer) readArray() (*RESP, int, error) {
	strLen, err := buf.reader.ReadString('\n')
	n := len(strLen)
	if err != nil {
		return nil, n, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(strLen, "\r\n"))
	if err != nil {
		return nil, n, err
	}
	if length == -1 {
		return nil, n, nil
	}

	values := make([]*RESP, length)
	for i := range length {
		value, m, err := buf.Read()
		if err != nil {
			return nil, n, err
		}
		n += m
		values[i] = value
	}

	return &RESP{
		Type:   ARRAY,
		Values: values,
	}, n, nil
}

func (buf *Buffer) readBulkString() (*RESP, int, error) {
	strLen, err := buf.reader.ReadString('\n')
	n := len(strLen)
	if err != nil {
		return &RESP{}, n, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(strLen, "\r\n"))
	if err != nil {
		return &RESP{}, n, err
	}
	if length == -1 {
		return &RESP{}, n, nil
	}

	data := make([]byte, length+2)
	m, err := io.ReadFull(buf.reader, data)
	if err != nil {
		return &RESP{}, n - (n - m), err
	}

	resp := &RESP{
		Type:  BULK,
		Value: string(data[:length]),
	}
	return resp, n + m, nil
}

func (buf *Buffer) readString() (*RESP, int, error) {
	data, err := buf.reader.ReadString('\n')
	if err != nil {
		return &RESP{}, 0, err
	}

	n := len(data)

	data = strings.TrimSuffix(data, "\r\n")
	return &RESP{
		Type:  STRING,
		Value: data,
	}, n, nil
}

func (buf *Buffer) readInteger() (*RESP, int, error) {
	num, err := buf.reader.ReadString('\n')
	if err != nil {
		return &RESP{}, 0, err
	}

	n := len(num)

	num = strings.TrimSuffix(num, "\r\n")
	return &RESP{
		Type:  INTEGER,
		Value: num,
	}, n, nil
}

func (buf *Buffer) ReadFullResync() (*RESP, error) {
	typ, err := buf.reader.ReadByte()
	if typ != STRING || err != nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("invalid resync")
	}

	data, err := buf.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(data, "FULLRESYNC") {
		return nil, errors.New("invalid resync")
	}

	return buf.readRDB()
}

func (buf *Buffer) readRDB() (*RESP, error) {
	typ, err := buf.reader.ReadByte()
	if typ != BULK || err != nil {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("invalid rdb")
	}

	strLen, err := buf.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(strLen, "\r\n"))
	if err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(buf.reader, data)
	if err != nil {
		return nil, err
	}

	resp := &RESP{
		Type:  RDB,
		Value: string(data),
	}
	return resp, nil
}

// ----------------------------------------------------------------------------

// Serialize ------------------------------------------------------------------
type Writable interface {
	*RESP | []byte
}

func Write[T Writable](w *Writer, resp T) (int, error) {
	switch r := any(resp).(type) {
	case *RESP:
		bytes := r.Marshal()

		n, err := w.writer.Write(bytes)
		if err != nil {
			return n, err
		}

		return n, nil
	case []byte:
		n, err := w.writer.Write(r)
		if err != nil {
			return n, err
		}

		return n, nil
	default:
		return 0, nil
	}
}

func (resp *RESP) Marshal() []byte {
	switch resp.Type {
	case STRING:
		return resp.marshalString()
	case BULK:
		return resp.marshalBulk()
	case ARRAY:
		return resp.marshalArray()
	case RDB:
		return resp.marshallRDB()
	case ERROR:
		return resp.marshalError()
	case INTEGER:
		return resp.marshalInteger()
	default:
		return resp.marshalNull()
	}
}

func (resp *RESP) marshalString() (bytes []byte) {
	bytes = append(bytes, STRING)
	bytes = append(bytes, resp.Value...)
	bytes = append(bytes, CRLF...)
	return bytes
}

func (resp *RESP) marshalBulk() (bytes []byte) {
	bytes = append(bytes, BULK)
	bytes = strconv.AppendInt(bytes, int64(len((resp.Value))), 10)
	bytes = append(bytes, CRLF...)
	bytes = append(bytes, resp.Value...)
	bytes = append(bytes, CRLF...)
	return bytes
}

func (resp *RESP) marshalArray() (bytes []byte) {
	len := len(resp.Values)
	bytes = append(bytes, ARRAY)
	bytes = strconv.AppendInt(bytes, int64(len), 10)
	bytes = append(bytes, CRLF...)

	for i := range len {
		bytes = append(bytes, resp.Values[i].Marshal()...)
	}

	return bytes
}

func (resp *RESP) marshallRDB() (bytes []byte) {
	bytes = append(bytes, BULK)

	content := []byte{}
	for i := 0; i < len(resp.Value); i += 2 {
		tmp := resp.Value[i : i+2]
		hex, _ := strconv.ParseUint(tmp, 16, 8)
		content = append(content, byte(hex))
	}

	bytes = strconv.AppendInt(bytes, int64(len(content)), 10)
	bytes = append(bytes, CRLF...)
	bytes = append(bytes, content...)

	return bytes
}

func (resp *RESP) marshalError() (bytes []byte) {
	bytes = append(bytes, ERROR)
	bytes = append(bytes, resp.Value...)
	bytes = append(bytes, CRLF...)

	return bytes
}

func (resp *RESP) marshalInteger() (bytes []byte) {
	bytes = append(bytes, INTEGER)
	bytes = append(bytes, resp.Value...)
	bytes = append(bytes, CRLF...)

	return bytes
}

func (resp *RESP) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// ----------------------------------------------------------------------------
