package app

import (
	"errors"
	"strings"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/util"
)

type KVFunc func(req []byte, rsp *[]byte)

var cmdMap = map[string]KVFunc{
	// for keys
	"DEL":    DelHandler,
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
	if _, ok := cmdMap[strings.ToUpper(util.BytesToString(command))]; ok {
		return
	}
	err = ErrNotSupportCommand
	return
}
