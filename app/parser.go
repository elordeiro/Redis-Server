package main

import (
	"bufio"
	"errors"

	"io"
	"strconv"
	"strings"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
	NULL    = '_'
)

var CRLF = []byte("\r\n")

type RESP struct {
	Type   byte
	Value  string
	Values []*RESP
}

// Reader and Writer ----------------------------------------------------------
type Buffer struct {
	reader *bufio.Reader
}

type Writer struct {
	writer io.Writer
}

func NewBuffer(rd io.Reader) *Buffer {
	return &Buffer{
		reader: bufio.NewReader(rd),
	}
}

func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		writer: wr,
	}
}

// ----------------------------------------------------------------------------

// Deserialize ----------------------------------------------------------------
func (buf *Buffer) readBulkString() (*RESP, error) {
	strLen, err := buf.reader.ReadString('\n')
	if err != nil {
		return &RESP{}, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(strLen, "\r\n"))
	if err != nil {
		return &RESP{}, err
	}
	if length == -1 {
		return &RESP{}, nil
	}

	data := make([]byte, length+2)
	_, err = io.ReadFull(buf.reader, data)
	if err != nil {
		return &RESP{}, err
	}

	resp := &RESP{
		Type:  BULK,
		Value: string(data[:length]),
	}
	return resp, nil
}

func (buf *Buffer) readArray() (*RESP, error) {
	strLen, err := buf.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(strings.TrimSuffix(strLen, "\r\n"))
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}

	values := make([]*RESP, length)
	for i := range length {
		value, err := buf.Read()
		if err != nil {
			return nil, err
		}
		values[i] = value
	}

	return &RESP{
		Type:   ARRAY,
		Values: values,
	}, nil
}

func (buf *Buffer) Read() (*RESP, error) {
	typ, err := buf.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch typ {
	case ARRAY:
		return buf.readArray()
	case BULK:
		return buf.readBulkString()
	case STRING, ERROR, INTEGER:
		return nil, nil
	default:
		return nil, errors.New("invalid type")
	}
}

// ----------------------------------------------------------------------------

// Serialize ------------------------------------------------------------------
func (w *Writer) Write(resp *RESP) (int, error) {
	bytes := resp.Marshal()

	n, err := w.writer.Write(bytes)
	if err != nil {
		return n, err
	}

	return n, nil
}

func (resp *RESP) Marshal() []byte {
	switch resp.Type {
	case STRING:
		return resp.marshalString()
	case BULK:
		return resp.marshalBulk()
	case ARRAY:
		return resp.marshalArray()
	case ERROR:
		return resp.marshalError()
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

func (resp *RESP) marshalError() (bytes []byte) {
	bytes = append(bytes, ERROR)
	bytes = append(bytes, resp.Value...)
	bytes = append(bytes, CRLF...)

	return bytes
}

func (resp *RESP) marshalNull() []byte {
	return []byte("$-1\r\n")
}

// ----------------------------------------------------------------------------
