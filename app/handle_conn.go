package app

import "errors"

var (
	newConCmd = "*1\r\n$7\r\nCOMMAND\r\n"
	newConRsp = []byte("+OK\r\n")
	notNewCon = errors.New("- not new conn\r\n")
)

func HandleNewConn(frame []byte) ([]byte, error) {
	if newConCmd == string(frame) {
		return newConRsp, nil
	}
	return nil, notNewCon
}
