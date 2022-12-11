package app

import (
	"context"

	"magicdb/engine"
	"magicdb/mapi"

	"github.com/labstack/echo/v4"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var __GITHASH__ = ""

type App struct{}

func NewApp() *App {
	return &App{}
}

func (app *App) GRPCAPIRegister(s *grpc.Server) {
	mapi.RegisterMagicdbServer(s, app)
}

func (app *App) EchoAPIRegister(e *echo.Echo) {
	e.POST("/api/get", app.GetEchoHandler)
	e.POST("/", app.PingEchoHandler)
	e.POST("/version", app.VersionEchoHandler)
}

func (app *App) Get(ctx context.Context, in *mapi.Request) (*mapi.Response, error) {
	stat := prome.NewStat("App.Get")
	defer stat.End()
	response := &mapi.Response{}
	key := in.GetKey()
	if len(key) == 0 {
		response.Msg = "key empty"
		response.Code = 404
		return response, nil
	}
	var features *sample.Features
	if len(in.GetTables()) == 0 {
		features = engine.EngineInstance.GetAll(key)
	} else {
		features = engine.EngineInstance.Get(key, in.GetTables())
	}

	if features == nil {
		stat.MarkErr()
		response.Msg = "not hit"
		response.Code = 404
	} else {
		response.Features = features
	}

	return response, nil
}

func (app *App) GetEchoHandler(c echo.Context) (err error) {
	stat := prome.NewStat("App.GetEchoHandler")
	defer stat.End()
	request := &mapi.Request{}
	if err = c.Bind(request); err != nil {
		stat.MarkErr()
		return err
	}
	response, err := app.Get(context.Background(), request)
	if err != nil {
		stat.MarkErr()
		return err
	}
	return c.JSON(200, response)
}

func (app *App) PingEchoHandler(c echo.Context) (err error) {
	return c.JSON(200, "OK")
}

func (app *App) VersionEchoHandler(c echo.Context) (err error) {
	return c.JSON(200, __GITHASH__)
}

func (app *App) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if engine.EngineInstance.IsServing() {
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (app *App) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	if engine.EngineInstance.IsServing() {
		server.Send(&grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		})
	}
	server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	})
	return nil
}
