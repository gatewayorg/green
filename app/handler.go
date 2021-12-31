package app

import (
	"bytes"
	"errors"
	"strings"
	"sync"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
	"github.com/sunvim/utils/workpool"
)

var (
	taskPool   = workpool.New(40960)
	ErrRequest = errors.New("- error request message\r\n")
	bufPool    = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer([]byte{})
		},
	}
)

func handler(frame []byte) []byte {
	var (
		err      error
		cmd, out []byte
	)
	if err = codec.Check(frame); err != nil {
		return tools.StringToBytes(ErrRequest.Error())
	}

	if out, err = HandleNewConn(frame); err == nil {
		return out
	}

	if cmd, err = checkCommand(frame); err != nil {
		return tools.StringToBytes(err.Error())
	}

	// write ahead log
	gWal.Write(frame)

	// cache handler
	cmdMap[strings.ToUpper(tools.BytesToStringFast(cmd))](frame, &out)

	return out
}
