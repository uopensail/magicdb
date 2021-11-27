package app

import (
	"context"
	"github.com/labstack/echo/v4"
	"github.com/uopensail/fuku-core/api"
	"github.com/uopensail/ulib/prome"
	"google.golang.org/grpc"
	"magicdb/manager"
)

var __GITHASH__ = ""

type App struct{}

func NewApp() *App {
	return &App{}
}

func (app *App) GRPCAPIRegister(s *grpc.Server) {
	api.RegisterFuKuServer(s, app)
	//注册健康检查
	//grpc_health_v1.RegisterHealthServer(s, app)
}

func (app *App) EchoAPIRegister(e *echo.Echo) {
	e.POST("/api/get", app.GetEchoHandler)
	e.POST("/", app.PingEchoHandler)
	e.POST("/version", app.VersionEchoHandler)
}

func (app *App) Get(ctx context.Context, in *api.FuKuRequest) (*api.FuKuResponse, error) {
	stat := prome.NewStat("App.Get")
	defer stat.End()
	response := &api.FuKuResponse{}
	features := manager.Implementation.Get(in.GetUserID())
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
	request := &api.FuKuRequest{}
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

//func (app *App) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
//	return &grpc_health_v1.HealthCheckResponse{
//		Status: grpc_health_v1.HealthCheckResponse_SERVING,
//	}, nil
//}
//
//func (app *App) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
//	return server.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING})
//}
