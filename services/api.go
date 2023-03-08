package services

import (
	"context"
	"magicdb/mapi"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/sample"
)

func (srv *Services) Get(ctx context.Context, in *mapi.Request) (*mapi.Response, error) {
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
		features = srv.dbEngine.GetAll(key)
	} else {
		features = srv.dbEngine.Get(key, in.GetTables())
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

type StatusResponse struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}

// RecommendHandler @Summary 获取命中的实验
// @BasePath /api/v1
// @Accept  json
// @Produce  json
// @Param payload body sunmaoapi.RecRequest true "RecRequest"
// @Success 200 {object} sunmaoapi.RecResponse
// @Failure 500 {object} model.StatusResponse
// @Failure 400 {object} model.StatusResponse
// @Router /get [post]
func (srv *Services) GetHandler(gCtx *gin.Context) {
	pStat := prome.NewStat("GetHandler")
	defer pStat.End()

	var postData mapi.Request
	if err := gCtx.ShouldBind(&postData); err != nil {
		gCtx.JSON(http.StatusInternalServerError, StatusResponse{
			Code: -1,
			Msg:  err.Error(),
		})
		return
	}

	ret, err := srv.Get(context.Background(), &postData)
	if err != nil {
		gCtx.JSON(http.StatusInternalServerError, StatusResponse{
			Code: -1,
			Msg:  err.Error(),
		})
		return
	}

	gCtx.JSON(http.StatusOK, ret)
	return
}
