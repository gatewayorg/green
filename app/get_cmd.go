package app

import (
	"bytes"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/util"
)

var (
	ErrNotExist = "+(nil)\r\n"
)

func GetHandler(req []byte, rsp *[]byte) {
	key, err := codec.ExtactKey(req)
	if err != nil {
		*rsp = append(*rsp, util.StringToBytes(ErrRequest)...)
		return
	}
	rs := gBytePool.Get()
	rs = rs[:0]

	// step 1: get the value form cache
	rs = gCache.Get(rs, key)
	// step 2: if not exist, then get the value from low level
	if len(rs) == 0 {
		rs, err = gKVClient.Get(key)
		if err != nil {
			log.Debug("not exist key: ", util.BytesToString(key))
			*rsp = append(*rsp, util.StringToBytes(ErrNotExist)...)
			return
		}
	}

	// format response data
	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	buf.Reset()
	wr := codec.NewWriter(buf)
	wr.WriteArg(rs)
	log.Debug("last rsp: ", buf.String())
	*rsp = append(*rsp, buf.Bytes()...)
}
