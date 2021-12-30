package cmd

import (
	"os"
	"syscall"

	"github.com/gatewayorg/green/app"
	"github.com/gatewayorg/green/pkg/log"
	"github.com/gatewayorg/green/share"
	"github.com/urfave/cli/v2"
)

func MainRun() {
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:  share.API_PORT,
			Value: "4321",
			Usage: "service port",
		},
		&cli.StringFlag{
			Name:  share.API_ADDR,
			Value: "127.0.0.1",
			Usage: "service address",
		},
		&cli.StringFlag{
			Name:  share.STATE_PORT,
			Value: "4322",
			Usage: "metric port",
		},
		&cli.StringFlag{
			Name:  share.STATE_ADDR,
			Value: "127.0.0.1",
			Usage: "metric address",
		},
		&cli.StringFlag{
			Name:    share.LOG_LEVEL,
			Aliases: []string{"l"},
			Value:   "info",
			Usage:   "value: debug/info/error/fatal",
		},
		&cli.StringFlag{
			Name:  share.GOSSIP_PORT,
			Value: "4323",
			Usage: "gossip service port",
		},
		&cli.StringFlag{
			Name:  share.GOSSIP_ADDR,
			Value: "127.0.0.1",
			Usage: "gossip service address",
		},
	}

	svr := cli.NewApp()
	svr.Action = app.MainRun
	svr.Flags = flags

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
