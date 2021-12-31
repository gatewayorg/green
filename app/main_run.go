package app

import (
	"context"
	"strings"

	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/share"
	"github.com/sunvim/utils/grace"
	"github.com/sunvim/utils/netpoll"
	"github.com/sunvim/utils/tools"
	"github.com/urfave/cli/v2"
)

func MainRun(c *cli.Context) error {
	// init global enviroment
	initEnv(c)

	ctx, service := grace.New(context.Background())
	service.Register(func() error {
		gWal.Close()
		return nil
	})

	tools.SafeGo(func() {
		listenAndServ(ctx, c)
	})

	service.Wait()
	return nil
}

func listenAndServ(ctx context.Context, c *cli.Context) {
	var handler = &netpoll.DataHandler{
		NoShared:   false,
		NoCopy:     true,
		BufferSize: share.MAX_REQUEST_SIZE,
		HandlerFunc: func(req []byte) (res []byte) {
			taskPool.DoWait(func() error {
				res = handler(req)
				return nil
			})
			return
		},
	}
	log.Info("service booting with ", c.String(share.API_ADDR), ":", c.String(share.API_PORT))
	if err := netpoll.ListenAndServe("tcp",
		strings.Join([]string{c.String(share.API_ADDR), c.String(share.API_PORT)}, ":"),
		handler); err != nil {
		panic(err)
	}
}
