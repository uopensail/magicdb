package services

import (
	"context"
	"magicdb/engine"
	"magicdb/mapi"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Services provides gRPC and HTTP services, integrating with a database engine.
type Services struct {
	mapi.UnimplementedMagicdbServer
	db *engine.DataBase
}

// NewServices creates a new Services instance with the provided database.
func NewServices(db *engine.DataBase) *Services {
	srv := &Services{
		db: db,
	}
	return srv
}

// RegisterGrpc registers the gRPC services and health checks.
func (srv *Services) RegisterGrpc(grpcS *grpc.Server) {
	mapi.RegisterMagicdbServer(grpcS, srv)
	// grpc_health_v1.RegisterHealthServer(grpcS, srv)
	zap.L().Info("gRPC services registered successfully.")
}

// RegisterGinRouter sets up HTTP routes for the Gin engine.
func (srv *Services) RegisterGinRouter(ginEngine *gin.Engine) {
	apiV1 := ginEngine.Group("api/v1")
	apiV1.POST("/get", srv.GetHandler)
	zap.L().Info("HTTP routes registered successfully.")
}

// Check implements the health check interface for gRPC.
func (srv *Services) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	zap.L().Info("Health check requested.", zap.String("service", req.Service))
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// Watch implements the streaming health check interface for gRPC.
func (srv *Services) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	zap.L().Info("Health watch started.", zap.String("service", req.Service))
	err := server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
	if err != nil {
		zap.L().Error("Failed to send health status.", zap.Error(err))
		return err
	}
	return nil
}

// Close performs necessary cleanup when the Services instance is no longer needed.
func (srv *Services) Close() {
	zap.L().Info("Performing cleanup before shutdown.")
}
