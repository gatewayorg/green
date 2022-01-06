package app

import "github.com/sunvim/utils/tools"

const (
	pongRsp = "+Pong\r\n"
)

func PingHandler(req []byte, rsp *[]byte) {
	*rsp = tools.StringToBytes(pongRsp)
}
