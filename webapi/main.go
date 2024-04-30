package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/data-preservation-programs/spade/internal/app"
	logging "github.com/ipfs/go-log/v2"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ribasushi/go-toolbox/cmn"
	"github.com/ribasushi/go-toolbox/ufcli"
)

func setup() *echo.Echo {
	//
	// Server setup
	e := echo.New()

	// logging middleware must be first
	// TODO: unify with the ipfs logger below
	e.Logger.SetLevel(2) // https://github.com/labstack/gommon/blob/v0.4.0/log/log.go#L40-L42
	e.Use(middleware.LoggerWithConfig(
		middleware.LoggerConfig{
			Skipper:          middleware.DefaultSkipper,
			CustomTimeFormat: "2006-01-02 15:04:05.000",
			Format:           logCfg,
		},
	))

	// routes
	registerRoutes(e)

	//
	// Housekeeping
	e.HideBanner = true
	e.HidePort = true
	e.JSONSerializer = new(rawJSONSerializer)
	e.Any("*", retInvalidRoute)

	return e
}

type rawJSONSerializer struct{}

func (rawJSONSerializer) Serialize(c echo.Context, i interface{}, indent string) error {
	enc := json.NewEncoder(c.Response())
	enc.SetEscapeHTML(false)
	if indent != "" {
		enc.SetIndent("", indent)
	}
	return enc.Encode(i)
}

var defJSONSerializer = echo.DefaultJSONSerializer{}

func (rawJSONSerializer) Deserialize(c echo.Context, i interface{}) error {
	return defJSONSerializer.Deserialize(c, i)
}

func main() {
	cmdName := app.AppName + "-webapi"
	log := logging.Logger(fmt.Sprintf("%s(%d)", cmdName, os.Getpid()))

	// *always* log json
	{
		lcfg := logging.GetConfig()
		lcfg.Format = logging.JSONOutput
		logging.SetupLogging(lcfg)
		logging.SetLogLevel("*", "INFO")
	}

	home, err := os.UserHomeDir()
	if err != nil {
		log.Error(cmn.WrErr(err))
		os.Exit(1)
	}

	var e *echo.Echo
	(&ufcli.UFcli{
		Logger:   log,
		TOMLPath: fmt.Sprintf("%s/%s.toml", home, app.AppName),
		AppConfig: ufcli.App{
			Name: cmdName,
			Action: func(cctx *ufcli.Context) error {
				e = setup()
				e.Server.BaseContext = func(net.Listener) context.Context { return cctx.Context }
				return e.Start(cctx.String("webapi-listen-address"))
			},
			Flags: append(
				[]ufcli.Flag{
					ufcli.ConfStringFlag(&ufcli.StringFlag{
						Name:  "webapi-listen-address",
						Value: "localhost:8080",
					}),
				},
				app.CommonFlags...,
			),
		},
		GlobalInit: app.GlobalInit,
		BeforeShutdown: func() error {
			if e != nil {
				return e.Close()
			}
			return nil
		},
	}).RunAndExit(context.Background())
}

var logCfg = fmt.Sprintf("{%s}\n", strings.Join([]string{
	`"timestamp":"${time_custom}"`,
	`"req_uuid":"${header:X-SPADE-REQUEST-UUID}"`,
	`"error":"${error_stacktrace}"`,
	`"http_status":${status}`,
	`"fail_slug":"${header:X-SPADE-FAILURE-SLUG}"`,
	`"took":"${latency_human}"`,
	`"sp":"${header:X-SPADE-LOGGED-SP}"`,
	`"bytes_in":${bytes_in}`,
	`"bytes_out":${bytes_out}`,
	`"op":"${method} ${host}${uri}"`,
	// `"remote_ip":"${remote_ip}"`,
	`"user_agent":"${user_agent}"`,
}, ","))
