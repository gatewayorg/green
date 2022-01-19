package app

import (
	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
)

func DelHandler(req []byte, rsp *[]byte) {
	key, err := codec.ExtactKey(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}
	// write ahead log
	gWal.Write(req)

	gCache.Del(key)
	gKVClient.Del(key)

	*rsp = append(*rsp, tools.StringToBytes(okRsp)...)
}
