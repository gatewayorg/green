package app

import (
	"os"
	"time"

	"github.com/gatewayorg/green/pkg/cache"
	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/wal"
	"github.com/gatewayorg/green/share"
	"github.com/oxtoacart/bpool"
	"github.com/sunvim/utils/tools"
	"github.com/urfave/cli/v2"
)

var (
	gCache cache.Cacher
	gWal   *wal.WAL
)

func initEnv(c *cli.Context) {
	var err error
	gCache = cache.New(c.Int(share.CACHE_SIZE))
	os.MkdirAll(c.String(share.WAL_DIR), 0766)

	gWal, err = wal.Open(c.String(share.WAL_DIR), time.Duration(c.Int(share.WAL_SYNC_INTERVAL)))
	if err != nil {
		panic(err)
	}

	// load wal data into cache
	loadWal(gWal, gCache)
}

func loadWal(w *wal.WAL, c cache.Cacher) {
	log.Info("reload data from wal starting ...")

	bufferPool := bpool.NewBytePool(1, 65536)
	wr, err := w.NewReader("reload", nil, bufferPool.Get)
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
		c.Set(key, val)
	}

	log.Info("cache reload over!")

}
