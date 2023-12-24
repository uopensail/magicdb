package main

import (
	"flag"
	"fmt"
	"magicdb/services"
	"strings"

	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	etcd "github.com/go-kratos/kratos/contrib/registry/etcd/v2"
	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/registry"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	"google.golang.org/grpc"

	"github.com/gin-gonic/gin"

	"magicdb/config"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	etcdclient "go.etcd.io/etcd/client/v3"
)

var __GITCOMMITINFO__ = ""

// PingPongHandler @Summary 获取标签列表
// @BasePath /
// @Produce  json
// @Success 200 {object} model.StatusResponse
// @Router /ping [get]
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
	return
}

// @Summary 获取标签列表
// @BasePath /
// @Produce  json
// @Success 200 {object} model.StatusResponse
// @Router /git_hash [get]
func GitHashHandler(gCtx *gin.Context) {
	pStat := prome.NewStat("GitHashHandler")
	defer pStat.End()

	gCtx.String(http.StatusOK, "git_info:"+__GITCOMMITINFO__)
	return
}

func buildInstance(app *kratos.App) *registry.ServiceInstance {
	instance := registry.ServiceInstance{}
	instance.ID = app.ID()
	instance.Name = app.Name()
	instance.Version = app.Version()
	instance.Endpoints = app.Endpoint()
	instance.Metadata = app.Metadata()
	return &instance
}

func run(configFilePath string, logDir string) *services.Services {
	folder := path.Dir(configFilePath)
	zlog.InitLogger(config.AppConfigInstance.ProjectName, config.AppConfigInstance.Debug, logDir)

	var etcdCli *etcdclient.Client
	if len(config.AppConfigInstance.ServerConfig.Endpoints) > 0 {
		client, err := etcdclient.New(etcdclient.Config{
			Endpoints: config.AppConfigInstance.ServerConfig.Endpoints,
		})
		if err != nil {
			zlog.LOG.Fatal("etcd error", zap.Error(err))
		} else {
			etcdCli = client
		}
	}

	options := make([]kratos.Option, 0)
	var reg registry.Registrar

	if etcdCli != nil {
		reg = etcd.New(etcdCli)
		options = append(options, kratos.Registrar(reg))
	}
	serverName := config.AppConfigInstance.ServerConfig.Name
	services := services.NewServices()
	grpcSrv := newGRPC(services.RegisterGrpc)
	httpSrv := newHTTPServe(config.AppConfigInstance.ProjectName, services.RegisterGinRouter)

	options = append(options, kratos.Name(serverName), kratos.Version(__GITCOMMITINFO__), kratos.Server(
		httpSrv,
		grpcSrv,
	))
	app := kratos.New(options...)

	services.Init(folder, etcdCli, *buildInstance(app))
	go func() {
		if err := app.Run(); err != nil {
			zlog.LOG.Fatal("run error", zap.Error(err))
		}
	}()

	return services
}

func registerProme(projectName string, ginEngine *gin.Engine) {
	promeExport := prome.NewExporter(projectName)
	err := prometheus.Register(promeExport)

	if err != nil {
		zlog.LOG.Error("register prometheus fail ", zap.Error(err))
		return
	}
	ginEngine.GET("/metrics", gin.WrapH(promhttp.Handler()))

}

func newHTTPServe(projectName string, registerFunc func(*gin.Engine)) *khttp.Server {
	ginEngine := gin.New()
	ginEngine.Use(gin.Recovery())

	url := ginSwagger.URL(fmt.Sprintf("swagger/doc.json")) // The url pointing to API definition
	ginEngine.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, url))

	ginEngine.GET("/ping", PingPongHandler)
	ginEngine.GET("/git_hash", GitHashHandler)
	registerProme(projectName, ginEngine)

	registerFunc(ginEngine)
	httpSrv := khttp.NewServer(khttp.Address(fmt.Sprintf(":%d", config.AppConfigInstance.ServerConfig.HttpServerConfig.HTTPPort)))
	httpSrv.HandlePrefix("/", ginEngine)
	return httpSrv
}

func newGRPC(registerFunc func(server *grpc.Server)) *kgrpc.Server {
	grpcSrv := kgrpc.NewServer(
		kgrpc.Address(fmt.Sprintf(":%d",
			config.AppConfigInstance.ServerConfig.GRPCPort)),
		kgrpc.Middleware(
			recovery.Recovery(),
		),
	)
	registerFunc(grpcSrv.Server)
	return grpcSrv
}

func runPProf(port int) {
	if port > 0 {
		go func() {
			fmt.Println(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), nil))
		}()
	}
}

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
func initConfig(configFilePath string, httpPort, grpcPort int, projectName, endpoints string) {
	if ok, _ := pathExists(configFilePath); ok {
		config.AppConfigInstance.Init(configFilePath)
	} else {
		config.AppConfigInstance.Debug = false
		config.AppConfigInstance.HTTPPort = httpPort
		config.AppConfigInstance.GRPCPort = grpcPort
		config.AppConfigInstance.Endpoints = strings.Split(endpoints, ",")
		config.AppConfigInstance.Name = projectName
		config.AppConfigInstance.ServerConfig.ProjectName = projectName
	}

}

// @title Swagger Example API
// @version 1.0
// @description This is a sample server Petstore server.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /api/v1

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

func main() {
	configFilePath := flag.String("config", "conf/config.toml", "启动命令请设置配置文件目录")
	logDir := flag.String("log", "./logs", "log dir")
	projectName := flag.String("name", "magicdb", "server name, example: 'magicdb_engine")
	httpPort := flag.Int("http-port", 6528, "set http api port")
	grpcPort := flag.Int("grpc-port", 6527, "set grpc api port")
	etcdEndpoint := flag.String("endpoints", "127.0.0.1:2379", "etcd endpoints example: '1.1.1.1:2379,2.2.2.2:2379'")

	flag.Parse()
	initConfig(*configFilePath, *httpPort, *grpcPort, *projectName, *etcdEndpoint)
	application := run(*configFilePath, *logDir)

	if len(config.AppConfigInstance.ProjectName) <= 0 {
		panic("config.ProjectName NULL")
	}

	runPProf(config.AppConfigInstance.PProfPort)

	//prome的打点
	signalChanel := make(chan os.Signal, 1)
	signal.Notify(signalChanel, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " app running....")
	<-signalChanel
	application.Close()

	fmt.Println(time.Now().Format("2006-01-02 15:04:05"), " app exit....")
}
