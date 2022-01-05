package app

import (
	"time"

	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/wal"
	"github.com/oxtoacart/bpool"
	"github.com/sunvim/utils/tools"
)

var (
	timeSeg = 3 * time.Hour
)

// sync data into low level db, cache remain one hour data
func sync_data() {
	bufferPool := bpool.NewBytePool(1, 65536)

	for range time.Tick(timeSeg) {
		log.Debug("sync data start...")

		ts := wal.NewOffsetForTS(time.Now().Add(-1 * timeSeg))
		wr, err := gWal.NewReader("sync", ts, bufferPool.Get)
		if err != nil {
			log.Error(err)
			return
		}

		for {

			data, err := wr.Read()
			if err != nil {
				log.Error(err)
				break
			}
			key, val, err := codec.ExtactKeyAndValue(data)
			if err != nil {
				log.Error("error command record: ", tools.BytesToStringFast(data))
				break
			}
			gKVClient.Set(key, val)
		}

		gWal.TruncateBefore(ts)

		log.Debug("sync data over!")
	}

}
