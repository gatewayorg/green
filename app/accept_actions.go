package app

import (
	"errors"
	"strings"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
)

var cmdMap = map[string]struct{}{
	// for keys
	"DEL":    {},
	"EXISTS": {},
	// for strings
	"SET": {},
	"GET": {},
	// for Pub/Sub
	"PSUBSCRIBE":   {},
	"PUBSUB":       {},
	"PUBLISH":      {},
	"PUNSUBSCRIBE": {},
	"SUBSCRIBE":    {},
	"UNSUBSCRIBE":  {},
	// for admin
	"PING": {},
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
