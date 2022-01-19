package app

import (
	"time"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/go-redis/redis/v8"
	"github.com/sunvim/utils/tools"
)

const (
	CHAN_SIZE = 512
)

var (
	slaveMsgChan = make(chan []byte, CHAN_SIZE)
	addrm        = make(map[string]*redis.Client)
)

// msg:  nodeid:addr:port
func SlaveHandler(req []byte, rsp *[]byte) {
	_, val, err := codec.ExtactKeyAndValue(req)
	if err != nil {
		*rsp = append(*rsp, tools.StringToBytes(ErrRequest.Error())...)
		return
	}

	*rsp = tools.StringToBytes(okRsp)

	taskPool.Do(func() error {
		addr := tools.BytesToStringFast(val)
		cli := redis.NewClient(&redis.Options{
			Addr: addr,
		})
		addrm[addr] = cli
		// send message where sync data from
		tick := time.Tick(time.Second)
		for range tick {
			println(time.Now().String())
		}
		return nil
	})

	return

}
