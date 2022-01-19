package app

import (
	"os"
	"strings"
	"time"

	"github.com/gatewayorg/green/pkg/cache"
	"github.com/gatewayorg/green/pkg/codec"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/pkg/store"
	"github.com/gatewayorg/green/pkg/store/pebble"
	"github.com/gatewayorg/green/pkg/util"
	"github.com/gatewayorg/green/pkg/wal"
	"github.com/gatewayorg/green/share"
	"github.com/oxtoacart/bpool"
	"github.com/urfave/cli/v2"
)

var (
	gCache    cache.ICache
	gWal      *wal.WAL
	gKVClient store.IKV
	gBytePool *bpool.BytePool
)

const MAX_SINGLE_SIZE = 64 * 1024

func initEnv(c *cli.Context) {
	var err error
	gBytePool = bpool.NewBytePool(MAX_WORKER_SIZE, MAX_SINGLE_SIZE)
	gCache = cache.New(c.Int(share.CACHE_SIZE))
	os.MkdirAll(c.String(share.WAL_DIR), 0766)
	// low level store
	gKVClient = pebble.New()

	gWal, err = wal.Open(c.String(share.WAL_DIR), time.Duration(c.Int(share.WAL_SYNC_INTERVAL)))
	if err != nil {
		panic(err)
	}

	// load wal data into cache
	loadWal(gWal, gCache)
}

func loadWal(w *wal.WAL, c cache.ICache) {
	log.Info("reload data from wal starting ...")

	wBytePool := bpool.NewBytePool(1, MAX_SINGLE_SIZE)
	wr, err := w.NewReader("reload", nil, wBytePool.Get)
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
		cmd, err := codec.ExtractCommand(data)
		if err != nil {
			log.Error("error command record: ", util.BytesToString(data))
			break
		}
		switch strings.ToUpper(util.BytesToString(cmd)) {
		case "SET":
			key, val, _ := codec.ExtactKeyAndValue(data)
			c.Set(key, val)
		case "DEL":
			key, _ := codec.ExtactKey(data)
			c.Del(key)
		default:
			continue
		}
	}

	log.Info("cache reload over!")
}
