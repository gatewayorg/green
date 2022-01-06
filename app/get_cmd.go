package app

import (
	"bytes"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/sunvim/utils/tools"
)

func GetHandler(req []byte, rsp *[]byte) {
	key, err := codec.ExtactKey(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}

	// step 1: get the value form cache
	*rsp = gCache.Get(nil, key)
	// step 2: if not exist, then get the value from low level
	if *rsp == nil {
		*rsp, err = gKVClient.Get(key)
		if err != nil {
			log.Debug("not exist key: ", tools.BytesToStringFast(key))
		}
	}
	// format response data
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	wr := codec.NewWriter(buf)
	wr.WriteArg(*rsp)
	*rsp = buf.Bytes()
}
