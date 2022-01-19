package app

import (
	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/util"
)

func SetHandler(req []byte, rsp *[]byte) {
	key, val, err := codec.ExtactKeyAndValue(req)
	if err != nil {
		*rsp = append(*rsp, util.StringToBytes(ErrRequest.Error())...)
		return
	}
	log.Debug("key: ", util.BytesToString(key), " val: ", util.BytesToString(val))
	// write ahead log
	gWal.Write(req)

	gCache.Set(key, val)
	*rsp = append(*rsp, util.StringToBytes(okRsp)...)
}
