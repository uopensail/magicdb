package main

import (
	"flag"
	"fmt"
	"magicdb/engine"
	"magicdb/engine/model"
	"magicdb/services"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"magicdb/config"

	"github.com/gin-gonic/gin"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
)

var __GITCOMMITINFO__ = ""

// PingPongHandler provides a simple health check endpoint.
func PingPongHandler(gCtx *gin.Context) {
	pStat := prome.NewStat("PingPongHandler")
	defer pStat.End()

	gCtx.JSON(http.StatusOK, struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}{
		Code: 0,
		Msg:  "PONG",
	})
}

// GitHashHandler returns the current git commit hash of the service.
func GitHashHandler(gCtx *gin.Context) {
	pStat := prome.NewStat("GitHashHandler")
	defer pStat.End()

	gCtx.String(http.StatusOK, "git_info:"+__GITCOMMITINFO__)
}

// run initializes and starts the services (HTTP and gRPC).
func run(logDir string) *services.Services {
	// Initialize the logger
	zlog.InitLogger(config.AppConfigInstance.ProjectName, config.AppConfigInstance.Debug, logDir)

	// Load database configuration
	dbConfig, err := model.LoadDataBaseConfig(config.AppConfigInstance.DataBaseConfig)
	if err != nil {
		zlog.LOG.Warn("Failed to load database configuration", zap.Error(err))
	}

	// Initialize the database
	var db *engine.DataBase
	if dbConfig != nil {
		db = engine.NewDataBase(dbConfig)
	}

	// Initialize services
	services := services.NewServices(db)
	grpcSrv := newGRPC(services.RegisterGrpc)
	httpSrv := newHTTPServe(config.AppConfigInstance.ProjectName, services.RegisterGinRouter)

	// Create and start the application
	options := []kratos.Option{
		kratos.Name(config.AppConfigInstance.ServerConfig.Name),
		kratos.Version(__GITCOMMITINFO__),
		kratos.Server(httpSrv, grpcSrv),
	}
	app := kratos.New(options...)

	go func() {
		if err := app.Run(); err != nil {
			zlog.LOG.Fatal("Application run error", zap.Error(err))
		}
	}()

	return services
}

// registerProme registers Prometheus metrics handler.
func registerProme(projectName string, ginEngine *gin.Engine) {
	promeExport := prome.NewExporter(projectName)
	if err := prometheus.Register(promeExport); err != nil {
		zlog.LOG.Error("Failed to register Prometheus exporter", zap.Error(err))
		return
	}
	ginEngine.GET("/metrics", gin.WrapH(promhttp.Handler()))
}

// newHTTPServe creates a new HTTP server and registers routes.
func newHTTPServe(projectName string, registerFunc func(*gin.Engine)) *khttp.Server {
	ginEngine := gin.New()
	ginEngine.Use(gin.Recovery())

	// Register default routes
	ginEngine.GET("/ping", PingPongHandler)
	ginEngine.GET("/git_hash", GitHashHandler)
	registerProme(projectName, ginEngine)

	// Register custom routes
	registerFunc(ginEngine)

	// Create HTTP server
	httpSrv := khttp.NewServer(khttp.Address(fmt.Sprintf(":%d", config.AppConfigInstance.ServerConfig.HttpServerConfig.HTTPPort)))
	httpSrv.HandlePrefix("/", ginEngine)
	return httpSrv
}

// newGRPC creates a new gRPC server.
func newGRPC(registerFunc func(server *grpc.Server)) *kgrpc.Server {
	grpcSrv := kgrpc.NewServer(
		kgrpc.Address(fmt.Sprintf(":%d", config.AppConfigInstance.ServerConfig.GRPCPort)),
		kgrpc.Middleware(recovery.Recovery()),
	)
	registerFunc(grpcSrv.Server)
	return grpcSrv
}

// runPProf starts the PProf server for performance profiling.
func runPProf(port int) {
	if port <= 0 {
		zlog.LOG.Warn("PProf port is not set or invalid, skipping PProf setup")
		return
	}
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), nil); err != nil {
			zlog.LOG.Error("Failed to start PProf server", zap.Error(err))
		}
	}()
}

// initConfig initializes the application configuration.
func initConfig(configFilePath string) {
	if ok, _ := pathExists(configFilePath); ok {
		config.AppConfigInstance.Init(configFilePath)
	} else {
		panic(fmt.Sprintf("Configuration file not found: %s", configFilePath))
	}
}

// pathExists checks if a file or directory exists.
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// main is the entry point of the application.
func main() {
	// Parse command-line arguments
	configFilePath := flag.String("config", "conf/local/config.toml", "Path to the configuration file")
	logDir := flag.String("log", "./logs", "Log directory")
	flag.Parse()

	// Initialize configuration
	initConfig(*configFilePath)

	// Start the application
	application := run(*logDir)

	// Start PProf if enabled
	runPProf(config.AppConfigInstance.PProfPort)

	// Handle OS signals for graceful shutdown
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Application running...")
	<-signalChannel

	// Shutdown the application
	application.Close()
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Application exited")
}
