package app

import (
	"os"
	"time"

	"github.com/gatewayorg/green/pkg/cache"
	"github.com/gatewayorg/green/pkg/wal"
	"github.com/gatewayorg/green/share"
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
}
