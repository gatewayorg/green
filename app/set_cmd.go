package app

import (
	"github.com/gatewayorg/green/pkg/codec"
	"github.com/sunvim/utils/tools"
)

func SetHandler(req []byte, rsp *[]byte) {
	key, val, err := codec.ExtactKeyAndValue(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}
	// write ahead log
	gWal.Write(req)

	gCache.Set(key, val)
	*rsp = append(*rsp, tools.StringToBytes(okRsp)...)

}
