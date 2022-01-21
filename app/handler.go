package app

import (
	"bytes"
	"strings"
	"sync"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/util"
	"github.com/sunvim/utils/workpool"
)

const (
	MAX_WORKER_SIZE = 8192
)

var (
	taskPool   = workpool.New(MAX_WORKER_SIZE)
	ErrRequest = "- error request message\r\n"
	bufPool    = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer([]byte{})
		},
	}
)

func handler(frame []byte) []byte {
	var (
		err error
		out []byte
	)
	if out, err = HandleNewConn(frame); err == nil {
		return out
	}

	cmdKey, err := codec.ExtractCommand(frame)
	if err != nil {
		return util.StringToBytes(ErrRequest)
	}

	out = gBytePool.Get()
	out = out[:0]
	// cache handler
	if cmd, ok := cmdMap[strings.ToUpper(util.BytesToString(cmdKey))]; ok {
		cmd(frame, &out)
	} else {
		return util.StringToBytes(ErrNotSupportCommand)
	}

	return out
}
