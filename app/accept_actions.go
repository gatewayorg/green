package app

import (
	"bytes"
	"errors"
	"strings"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
)

type KVFunc func(req []byte, rsp *[]byte)

var cmdMap = map[string]KVFunc{
	// for keys
	"DEL":    nil,
	"EXISTS": nil,
	// for strings
	"SET": SetHandler,
	"GET": GetHandler,
	// for Pub/Sub
	"PSUBSCRIBE":   nil,
	"PUBSUB":       nil,
	"PUBLISH":      nil,
	"PUNSUBSCRIBE": nil,
	"SUBSCRIBE":    nil,
	"UNSUBSCRIBE":  nil,
	// for admin
	"PING": nil,
}

func SetHandler(req []byte, rsp *[]byte) {
	key, val, err := codec.ExtactKeyAndValue(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}

	gCache.Set(key, val)
	*rsp = append(*rsp, tools.StringToBytes(okRsp)...)

}

func GetHandler(req []byte, rsp *[]byte) {
	key, err := codec.ExtactKey(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}

	*rsp = gCache.Get(nil, key)
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()

	wr := codec.NewWriter(buf)
	wr.WriteArg(*rsp)
	*rsp = buf.Bytes()
}

var (
	ErrNotSupportCommand = errors.New("- not support command\r\n")
)

func checkCommand(frame []byte) (command []byte, err error) {
	// check command
	command, err = codec.ExtractCommand(frame)
	if err != nil {
		err = ErrRequest
		return
	}
	if _, ok := cmdMap[strings.ToUpper(tools.BytesToStringFast(command))]; ok {
		return
	}
	err = ErrNotSupportCommand
	return
}
