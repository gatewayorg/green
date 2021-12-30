package codec

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/gatewayorg/green/pkg/util"
)

// redis resp protocol data type.
const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'
)

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

//------------------------------------------------------------------------------

type MultiBulkParse func(*Reader, int64) (interface{}, error)

type Reader struct {
	rd   *bufio.Reader
	_buf []byte
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:   bufio.NewReader(rd),
		_buf: make([]byte, 64),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// readLine that returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b, nil
}

var (
	ErrNotSupport = errors.New("not support")
	ErrInternel   = errors.New("system internel error")
	bs            = sync.Pool{
		New: func() interface{} {
			return []byte{}
		},
	}
)

func (r *Reader) readLineN(n int64) ([]byte, error) {
	rs := bs.Get().([]byte)
	defer bs.Put(rs)
	rs = rs[:0]
	double := n * 2
	var i int64
	for i = 0; i < double; i++ {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		rs = append(rs, line...)
	}
	return rs, nil
}

func (r *Reader) ReadAll() ([]byte, error) {

	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	if isNilReply(line) {
		return line, nil
	}
	lline := len(line)

	switch line[0] {

	default:
		return nil, ErrNotSupport

	case ErrorReply:
		return nil, RedisError(line[1 : lline-2])

	case StatusReply, IntReply:
		return line, nil

	case StringReply:
		lines, err := r.readLine()
		if err != nil {
			return nil, ErrInternel
		}

		rs := make([]byte, len(lines)+len(line))
		copy(rs, line)
		copy(rs[len(line):], lines)
		return rs, nil

	case ArrayReply:
		n, err := parseArrayLen(line[:lline-2])
		if err != nil {
			return nil, err
		}
		lines, err := r.readLineN(n)
		if err != nil {
			return nil, err
		}

		rs := make([]byte, len(lines)+len(line))
		copy(rs, line)
		copy(rs[len(line):], lines)
		return rs, nil

	}
}

func isNilReply(b []byte) bool {
	return len(b) == 5 &&
		(b[0] == StringReply || b[0] == ArrayReply) &&
		b[1] == '-' && b[2] == '1'
}

func parseArrayLen(line []byte) (int64, error) {
	if isNilReply(line) {
		return 0, Nil
	}
	return util.ParseInt(line[1:], 10, 64)
}
