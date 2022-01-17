package app

import (
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
	"PING": PingHandler,
	// for cluster
	"SLAVEOF": SlaveHandler,
	"SYNC":    SyncHandler,
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
