package app

import (
	"errors"

	"github.com/gatewayorg/green/pkg/util"
)

var (
	newConCmd = "*1\r\n$7\r\nCOMMAND\r\n"
	newConRsp = "+OK\r\n"
	okRsp     = "+OK\r\n"
	notNewCon = errors.New("- not new conn\r\n")
)

func HandleNewConn(frame []byte) ([]byte, error) {
	if newConCmd == util.BytesToString(frame) {
		return util.StringToBytes(newConRsp), nil
	}
	return nil, notNewCon
}
