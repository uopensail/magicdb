package main

import (
	"flag"
	"fmt"
	"magicdb/app"
	"magicdb/config"
	"magicdb/engine"
	"magicdb/register"
	"net"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func init() {
	configPath := flag.String("config", "conf/config.toml", "path of configure")
	flag.Parse()

	config.AppConfigImp.Init(*configPath)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   config.AppConfigImp.Etcdconfig.Address,
		DialTimeout: time.Duration(config.AppConfigImp.Etcdconfig.TTL) * time.Second,
	})
	if err != nil {
		panic(err)
	}
	register.Init(client)
	engine.Init(client)
}

func runGRPC() {
	go func() {
		app := app.NewApp()
		grpcServer := grpc.NewServer()
		//添加监控检测服务
		grpc_health_v1.RegisterHealthServer(grpcServer, app)
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

func main() {
	zlog.InitLogger(config.AppConfigImp.ProjectName,
		config.AppConfigImp.Debug,
		config.AppConfigImp.LogDir)

	runGRPC()
	runProme()
	runHttp()
}
