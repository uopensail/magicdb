package services

import (
	"context"
	"magicdb/config"
	"magicdb/engine"
	"magicdb/mapi"

	"github.com/gin-gonic/gin"
	"github.com/go-kratos/kratos/v2/registry"

	"github.com/uopensail/ulib/utils"
	etcdclient "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Services struct {
	mapi.UnimplementedMagicdbServer
	etcdCli  *etcdclient.Client
	dbEngine *engine.Engine
	instance registry.ServiceInstance
}

func NewServices() *Services {
	srv := Services{}

	return &srv
}
func (srv *Services) Init(configFolder string, etcdCli *etcdclient.Client, instance registry.ServiceInstance) {
	srv.etcdCli = etcdCli
	dbEngine := engine.NewEngine(config.AppConfigInstance.WorkDir,
		config.AppConfigInstance.CacheSize, etcdCli, instance)
	srv.dbEngine = dbEngine
	srv.instance = instance
}
func (srv *Services) RegisterGrpc(grpcS *grpc.Server) {

	mapi.RegisterMagicdbServer(grpcS, srv)
	//grpc_health_v1.RegisterHealthServer(grpcS, srv)
}

func (srv *Services) RegisterGinRouter(ginEngine *gin.Engine) {
	apiV1 := ginEngine.Group("api/v1")
	{
		apiV1.POST("/get", srv.GetHandler)

	}

}

func (srv *Services) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if srv.dbEngine.JobStatus() != utils.NormalJobStatus {
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (srv *Services) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	if srv.dbEngine.JobStatus() != utils.NormalJobStatus {
		server.Send(&grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		})
	}
	server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	})
	return nil
}
func (srv *Services) Close() {

}
