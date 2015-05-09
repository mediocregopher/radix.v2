// This package provides an easy to use interface for creating and parsing
// messages encoded in the REdis Serialization Protocol (RESP). You can check
// out more details about the protocol here: http://redis.io/topics/protocol
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

var (
	delim    = []byte{'\r', '\n'}
	delimEnd = delim[len(delim)-1]
)

type Type int

const (
	SimpleStr Type = iota
	Err
	Int
	BulkStr
	Array
	Nil
)

const (
	simpleStrPrefix = '+'
	errPrefix       = '-'
	intPrefix       = ':'
	bulkStrPrefix   = '$'
	arrayPrefix     = '*'
)

// Parse errors
var (
	badType  = errors.New("wrong type")
	parseErr = errors.New("parse error")
)

type Message struct {
	Type
	val interface{}
	raw []byte
}

// NewMessagePParses the given raw message and returns a Message struct
// representing it
func NewMessage(b []byte) (*Message, error) {
	return ReadMessage(bytes.NewReader(b))
}

// Can be used when writing to a resp stream to write a simple-string-style
// stream (e.g. +OK\r\n) instead of the default bulk-string-style strings.
//
// 	foo := NewSimpleString("foo")
// 	bar := NewSimpleString("bar")
// 	baz := NewSimpleString("baz")
// 	resp.WriteArbitrary(w, foo)
// 	resp.WriteArbitrary(w, []interface{}{bar, baz})
//
func NewSimpleString(s string) *Message {
	b := append(make([]byte, 0, len(s)+3), '+')
	b = append(b, []byte(s)...)
	b = append(b, '\r', '\n')
	return &Message{
		Type: SimpleStr,
		val:  s,
		raw:  b,
	}
}

// ReadMessage attempts to read a message object from the given io.Reader, parse
// it, and return a Message struct representing it
func ReadMessage(reader io.Reader) (*Message, error) {
	r := bufio.NewReader(reader)
	return bufioReadMessage(r)
}

func bufioReadMessage(r *bufio.Reader) (*Message, error) {
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	switch b[0] {
	case simpleStrPrefix:
		return readSimpleStr(r)
	case errPrefix:
		return readError(r)
	case intPrefix:
		return readInt(r)
	case bulkStrPrefix:
		return readBulkStr(r)
	case arrayPrefix:
		return readArray(r)
	default:
		return nil, badType
	}
}

func readSimpleStr(r *bufio.Reader) (*Message, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	return &Message{Type: SimpleStr, val: b[1 : len(b)-2], raw: b}, nil
}

func readError(r *bufio.Reader) (*Message, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	return &Message{Type: Err, val: b[1 : len(b)-2], raw: b}, nil
}

func readInt(r *bufio.Reader) (*Message, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	i, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, parseErr
	}
	return &Message{Type: Int, val: i, raw: b}, nil
}

func readBulkStr(r *bufio.Reader) (*Message, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, parseErr
	}
	if size < 0 {
		return &Message{Type: Nil, raw: b}, nil
	}
	total := make([]byte, size)
	b2 := total
	var n int
	for len(b2) > 0 {
		n, err = r.Read(b2)
		if err != nil {
			return nil, err
		}
		b2 = b2[n:]
	}

	// There's a hanging \r\n there, gotta read past it
	trail := make([]byte, 2)
	for i := 0; i < 2; i++ {
		if c, err := r.ReadByte(); err != nil {
			return nil, err
		} else {
			trail[i] = c
		}
	}

	blens := len(b) + len(total)
	raw := make([]byte, 0, blens+2)
	raw = append(raw, b...)
	raw = append(raw, total...)
	raw = append(raw, trail...)
	return &Message{Type: BulkStr, val: total, raw: raw}, nil
}

func readArray(r *bufio.Reader) (*Message, error) {
	b, err := r.ReadBytes(delimEnd)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(string(b[1:len(b)-2]), 10, 64)
	if err != nil {
		return nil, parseErr
	}
	if size < 0 {
		return &Message{Type: Nil, raw: b}, nil
	}

	arr := make([]*Message, size)
	for i := range arr {
		m, err := bufioReadMessage(r)
		if err != nil {
			return nil, err
		}
		arr[i] = m
		b = append(b, m.raw...)
	}
	return &Message{Type: Array, val: arr, raw: b}, nil
}

// Bytes returns a byte slice representing the value of the Message. Only valid
// for a Message of type SimpleStr, Err, and BulkStr. Others will return an
// error
func (m *Message) Bytes() ([]byte, error) {
	if b, ok := m.val.([]byte); ok {
		return b, nil
	}
	return nil, badType
}

// Str is a Convenience method around Bytes which converts the output to a
// string
func (m *Message) Str() (string, error) {
	b, err := m.Bytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Int returns an int64 representing the value of the Message. Only valid for
// Int messages
func (m *Message) Int() (int64, error) {
	if i, ok := m.val.(int64); ok {
		return i, nil
	}
	return 0, badType
}

// Err returns an error representing the value of the Message. Only valid for
// Err messages
func (m *Message) Err() (error, error) {
	if m.Type != Err {
		return nil, badType
	}
	s, err := m.Str()
	if err != nil {
		return nil, err
	}
	return errors.New(s), nil
}

// Array returns the Message slice encompassed by this Messsage, assuming the
// Message is of type Array
func (m *Message) Array() ([]*Message, error) {
	if a, ok := m.val.([]*Message); ok {
		return a, nil
	}
	return nil, badType
}

func writeBytesHelper(w io.Writer, b []byte, lastErr error) error {
	if lastErr != nil {
		return lastErr
	}
	_, err := w.Write(b)
	return err
}

// WriteMessage takes in the given Message and writes its encoded form to the
// given io.Writer
func WriteMessage(w io.Writer, m *Message) error {
	_, err := w.Write(m.raw)
	return err
}

// WriteArbitrary takes in any primitive golang value, or Message, and writes
// its encoded form to the given io.Writer, inferring types where appropriate.
func WriteArbitrary(w io.Writer, m interface{}) error {
	return write(w, m, false, false)
}

// WriteArbitraryAsString is similar to WriteArbitraryAsFlattenedString except
// that it won't flatten any embedded arrays.
func WriteArbitraryAsString(w io.Writer, m interface{}) error {
	return write(w, m, true, false)
}

// WriteArbitraryAsFlattenedStrings is similar to WriteArbitrary except that it
// will encode all types except Array as a BulkStr, converting the argument into
// a string first as necessary. It will also flatten any embedded arrays into a
// single long array. This is useful because commands to a redis server must be
// given as an array of bulk strings. If the argument isn't already in a slice
// or map it will be wrapped so that it is written as an Array of size one.
//
// Note that if a Message type is found it will *not* be encoded to a BulkStr,
// but will simply be passed through as whatever type it already represents.
func WriteArbitraryAsFlattenedStrings(w io.Writer, m interface{}) error {
	fl := flattenedLength(m)
	var err error
	err = writeBytesHelper(w, []byte("*"), err)
	err = writeBytesHelper(w, []byte(strconv.Itoa(fl)), err)
	err = writeBytesHelper(w, []byte("\r\n"), err)
	if err != nil {
		return err
	}

	return write(w, m, true, true)
}

func write(w io.Writer, m interface{}, forceString, flattened bool) error {
	switch mt := m.(type) {
	case []byte:
		return writeStr(w, mt)
	case string:
		return writeStr(w, []byte(mt))
	case bool:
		if mt {
			return writeStr(w, []byte("1"))
		} else {
			return writeStr(w, []byte("0"))
		}
	case nil:
		if forceString {
			return writeStr(w, []byte{})
		} else {
			return writeNil(w)
		}
	case int:
		return writeInt(w, int64(mt), forceString)
	case int8:
		return writeInt(w, int64(mt), forceString)
	case int16:
		return writeInt(w, int64(mt), forceString)
	case int32:
		return writeInt(w, int64(mt), forceString)
	case int64:
		return writeInt(w, mt, forceString)
	case uint:
		return writeInt(w, int64(mt), forceString)
	case uint8:
		return writeInt(w, int64(mt), forceString)
	case uint16:
		return writeInt(w, int64(mt), forceString)
	case uint32:
		return writeInt(w, int64(mt), forceString)
	case uint64:
		return writeInt(w, int64(mt), forceString)
	case float32:
		ft := strconv.FormatFloat(float64(mt), 'f', -1, 32)
		return writeStr(w, []byte(ft))
	case float64:
		ft := strconv.FormatFloat(mt, 'f', -1, 64)
		return writeStr(w, []byte(ft))
	case error:
		if forceString {
			return writeStr(w, []byte(mt.Error()))
		} else {
			return writeErr(w, mt)
		}

	// For the following cases, where we are writing an array, we only write the
	// array header (a new array) if flattened is false, otherwise we just write
	// each element inline and assume the array header has already been written

	// We duplicate the below code here a bit, since this is the common case and
	// it'd be better to not get the reflect package involved here
	case []interface{}:
		l := len(mt)
		lstr := strconv.Itoa(l)

		var err error

		if !flattened {
			err = writeBytesHelper(w, []byte("*"), err)
			err = writeBytesHelper(w, []byte(lstr), err)
			err = writeBytesHelper(w, []byte("\r\n"), err)
			if err != nil {
				return err
			}
		}

		for i := 0; i < l; i++ {
			if err = write(w, mt[i], forceString, flattened); err != nil {
				return err
			}
		}
		return nil

	case *Message:
		_, err := w.Write(mt.raw)
		return err

	default:
		// Fallback to reflect-based.
		switch reflect.TypeOf(m).Kind() {
		case reflect.Slice:
			rm := reflect.ValueOf(mt)
			l := rm.Len()
			lstr := strconv.Itoa(l)

			var err error

			if !flattened {
				err = writeBytesHelper(w, []byte("*"), err)
				err = writeBytesHelper(w, []byte(lstr), err)
				err = writeBytesHelper(w, []byte("\r\n"), err)
				if err != nil {
					return err
				}
			}

			for i := 0; i < l; i++ {
				vv := rm.Index(i).Interface()
				if err = write(w, vv, forceString, flattened); err != nil {
					return err
				}
			}
			return nil

		case reflect.Map:
			rm := reflect.ValueOf(mt)
			l := rm.Len() * 2
			lstr := strconv.Itoa(l)

			var err error

			if !flattened {
				err = writeBytesHelper(w, []byte("*"), err)
				err = writeBytesHelper(w, []byte(lstr), err)
				err = writeBytesHelper(w, []byte("\r\n"), err)
				if err != nil {
					return err
				}
			}

			keys := rm.MapKeys()
			for _, k := range keys {
				kv := k.Interface()
				vv := rm.MapIndex(k).Interface()
				if err = write(w, kv, forceString, flattened); err != nil {
					return err
				}
				if err = write(w, vv, forceString, flattened); err != nil {
					return err
				}
			}
			return nil

		default:
			return writeStr(w, []byte(fmt.Sprint(m)))
		}
	}
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

func flattenedLength(m interface{}) int {
	t := reflect.TypeOf(m)

	// If it's a byte-slice we don't want to flatten
	if t == typeOfBytes {
		return 1
	}

	total := 0

	switch t.Kind() {
	case reflect.Slice:
		rm := reflect.ValueOf(m)
		l := rm.Len()
		for i := 0; i < l; i++ {
			total += flattenedLength(rm.Index(i).Interface())
		}

	case reflect.Map:
		rm := reflect.ValueOf(m)
		keys := rm.MapKeys()
		for _, k := range keys {
			kv := k.Interface()
			vv := rm.MapIndex(k).Interface()
			total += flattenedLength(kv)
			total += flattenedLength(vv)
		}

	default:
		total++
	}

	return total
}

func writeStr(w io.Writer, b []byte) error {
	l := strconv.Itoa(len(b))
	var err error
	err = writeBytesHelper(w, []byte{bulkStrPrefix}, err)
	err = writeBytesHelper(w, []byte(l), err)
	err = writeBytesHelper(w, delim, err)
	err = writeBytesHelper(w, b, err)
	err = writeBytesHelper(w, delim, err)
	return err
}

func writeErr(w io.Writer, ierr error) error {
	ierrstr := []byte(ierr.Error())
	var err error
	err = writeBytesHelper(w, []byte{errPrefix}, err)
	err = writeBytesHelper(w, ierrstr, err)
	err = writeBytesHelper(w, delim, err)
	return err
}

func writeInt(w io.Writer, i int64, forceString bool) error {
	istr := strconv.FormatInt(i, 10)
	if forceString {
		return writeStr(w, []byte(istr))
	}
	var err error
	err = writeBytesHelper(w, []byte{intPrefix}, err)
	err = writeBytesHelper(w, []byte(istr), err)
	err = writeBytesHelper(w, delim, err)
	return err
}

var nilFormatted = []byte("$-1\r\n")

func writeNil(w io.Writer) error {
	_, err := w.Write(nilFormatted)
	return err
}
