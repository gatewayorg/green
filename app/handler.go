package app

import (
	"bytes"
	"errors"
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
		return util.StringToBytes(ErrRequest.Error())
	}

	if out, err = HandleNewConn(frame); err == nil {
		return out
	}

	if cmd, err = checkCommand(frame); err != nil {
		return util.StringToBytes(err.Error())
	}

	out = gBytePool.Get()
	out = out[:0]
	// cache handler
	cmdMap[strings.ToUpper(util.BytesToString(cmd))](frame, &out)

	return out
}
