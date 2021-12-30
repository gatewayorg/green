package app

import (
	"errors"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
	"github.com/sunvim/utils/workpool"
)

var (
	taskPool   = workpool.New(40960)
	ErrRequest = errors.New("- error request message\r\n")
)

func handler(frame []byte) (out []byte) {

	if err := codec.Check(frame); err != nil {
		out = tools.StringToBytes(ErrRequest.Error())
		return
	}

	if rsp, err := HandleNewConn(frame); err == nil {
		out = rsp
		return
	}

	return nil
}
