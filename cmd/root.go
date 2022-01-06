package cmd

import (
	"os"
	"syscall"

	"github.com/gatewayorg/green/app"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/share"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

func MainRun() {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  "config",
			Value: "config.toml",
			Usage: "global config file",
		},
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.API_PORT,
				Value: "4321",
				Usage: "service port",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.API_ADDR,
				Value: "127.0.0.1",
				Usage: "service address",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.STATE_PORT,
				Value: "4322",
				Usage: "metric port",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.STATE_ADDR,
				Value: "127.0.0.1",
				Usage: "metric address",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:    share.LOG_LEVEL,
				Aliases: []string{"l"},
				Value:   "info",
				Usage:   "value: debug/info/error/fatal",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.GOSSIP_PORT,
				Value: "4323",
				Usage: "gossip service port",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.GOSSIP_ADDR,
				Value: "127.0.0.1",
				Usage: "gossip service address",
			}),
		altsrc.NewIntFlag(
			&cli.IntFlag{
				Name:  share.CACHE_SIZE,
				Value: 2147483648,
				Usage: "cache max size",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.WAL_DIR,
				Value: "wal",
				Usage: "wal directory",
			}),
		altsrc.NewIntFlag(
			&cli.IntFlag{
				Name:  share.WAL_SYNC_INTERVAL,
				Value: 5,
				Usage: "sync wal log ervey n seconds",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.DATA_DIR,
				Value: "data",
				Usage: "database data path",
			}),
		altsrc.NewStringFlag(
			&cli.StringFlag{
				Name:  share.CLUSTER_SLAVE,
				Value: "",
				Usage: "if slave not null, then slave role",
			}),
	}

	svr := cli.NewApp()
	svr.Action = app.MainRun
	svr.Flags = flags
	svr.Before = altsrc.InitInputSourceWithContext(flags, altsrc.NewTomlSourceFromFlagFunc("config"))

	// { sub command list
	// append version command
	appendCmdList(svr, version)
	//}
	setupRLimit()
	err := svr.Run(os.Args)
	if err != nil {
		log.Fatal("Service Crash ", err)
	}
}

func appendCmdList(app *cli.App, subcmd *cli.Command) {
	app.Commands = append(app.Commands, subcmd)
}

func setupRLimit() {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		panic(err)
	}
	rlimit.Cur = rlimit.Max - 1000
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		panic(err)
	}
}
