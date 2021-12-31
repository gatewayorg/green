package app

import (
	"errors"

	"github.com/sunvim/utils/tools"
)

var (
	newConCmd = "*1\r\n$7\r\nCOMMAND\r\n"
	newConRsp = "+OK\r\n"
	okRsp     = "+OK\r\n"
	notNewCon = errors.New("- not new conn\r\n")
)

func HandleNewConn(frame []byte) ([]byte, error) {
	if newConCmd == string(frame) {
		return tools.StringToBytes(newConRsp), nil
	}
	return nil, notNewCon
}
