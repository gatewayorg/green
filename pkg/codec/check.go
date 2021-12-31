package codec

import (
	"bytes"
	"errors"
)

var (
	ErrReq = errors.New("- invalid message\r\n")
)

func Check(frame []byte) error {
	if len(frame) == 0 {
		return ErrReq
	}
	r := bytes.NewReader(frame)
	head, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch head {
	default:
		return ErrReq
	case ErrorReply:
		goto END
	case StatusReply, IntReply, StringReply, ArrayReply:
		goto END
	}
END:

	fl := len(frame)
	if frame[fl-1] == '\n' && frame[fl-2] == '\r' {
		return nil
	}

	return ErrReq
}

// ExtractKey extract key for dispatch backend server
func ExtactKey(frame []byte) (key []byte, err error) {

	reader := NewReader(bytes.NewReader(frame))

	// skip request message length
	reader.readLine()

	// skip key command length
	reader.readLine()

	// skip command
	reader.readLine()

	// skip key  length
	reader.readLine()

	// take key
	key, err = reader.readLine()
	if err != nil {
		return nil, err
	}
	key = key[:len(key)-2]
	return
}

// ExtractKeyAndValue extract key and value for dispatch backend server
func ExtactKeyAndValue(frame []byte) (key, value []byte, err error) {

	reader := NewReader(bytes.NewReader(frame))

	// skip request message length
	reader.readLine()

	// skip key command length
	reader.readLine()

	// skip command
	reader.readLine()

	// skip key  length
	reader.readLine()
	key, err = reader.readLine()
	if err != nil {
		return nil, nil, err
	}
	key = key[:len(key)-2]

	// skip value length
	reader.readLine()
	// take value
	value, err = reader.readLine()
	if err != nil {
		return nil, nil, err
	}
	value = value[:len(value)-2]

	return
}

func ExtractCommand(frame []byte) (key []byte, err error) {
	reader := NewReader(bytes.NewReader(frame))

	// skip request message length
	reader.readLine()

	// skip key command length
	reader.readLine()

	// take command
	key, err = reader.readLine()
	if err != nil {
		return nil, err
	}
	key = key[:len(key)-2]

	return
}
