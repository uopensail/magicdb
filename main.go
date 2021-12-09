package main

import (
	"flag"
	"fmt"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/robfig/cron/v3"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"magicdb/app"
	"magicdb/config"
	"magicdb/manager"
	"net"
)

func init() {
	configPath := flag.String("config", "conf/config.toml", "path of configure")
	flag.Parse()
	config.AppConfigImp.Init(*configPath)
	manager.Init()
	crontab()
}

func runGRPC() {
	go func() {
		app := app.NewApp()
		grpcServer := grpc.NewServer()
		//添加监控检测服务
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
		app.GRPCAPIRegister(grpcServer)
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.AppConfigImp.GRPCPort))
		if err != nil {
			zlog.SLOG.Fatal(err)
			panic(err)
		}

		err = grpcServer.Serve(listener)
		panic(err)
	}()
}

func runProme() {
	go func() {
		exporter := prome.NewExporter(config.AppConfigImp.ProjectName)
		err := exporter.Start(config.AppConfigImp.PromePort)
		if err != nil {
			zlog.SLOG.Fatal(err)
			panic(err)
		}
	}()
}

func runHttp() {
	e := echo.New()
	app := app.NewApp()
	e.Use(middleware.Recover())
	app.EchoAPIRegister(e)
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", config.AppConfigImp.HTTPPort)))
}

//crontab 定时任务
func crontab() {
	spec := "* * */7 * * *"
	c := cron.New(cron.WithSeconds())
	c.AddFunc(spec, manager.Implementation.Clean)
	c.Start()
}

func main() {
	zlog.InitLogger(config.AppConfigImp.ProjectName,
		config.AppConfigImp.Debug,
		config.AppConfigImp.LogPath)

	runGRPC()
	runProme()
	runHttp()
}