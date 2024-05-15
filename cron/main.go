package main

import (
	"context"
	"fmt"
	"os"

	"github.com/data-preservation-programs/spade/internal/app"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
)

func main() {
	cmdName := app.AppName + "-cron"
	log := logging.Logger(fmt.Sprintf("%s(%d)", cmdName, os.Getpid()))
	logging.SetLogLevel("*", "INFO")

	home, err := os.UserHomeDir()
	if err != nil {
		log.Error(cmn.WrErr(err))
		os.Exit(1)
	}

	(&ufcli.UFcli{
		Logger:   log,
		TOMLPath: fmt.Sprintf("%s/%s.toml", home, app.AppName),
		AppConfig: ufcli.App{
			Name:  cmdName,
			Usage: "Misc background processes for " + app.AppName,
			Commands: []*ufcli.Command{
				pollProviders,
				trackDeals,
				signPending,
				proposePending,
				bulkPiecePoll,
			},
			Flags: app.CommonFlags,
		},
		GlobalInit: app.GlobalInit,
	}).RunAndExit(context.Background())
}
