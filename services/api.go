package services

import (
	"context"
	"encoding/binary"
	"magicdb/engine"
	"magicdb/engine/table"
	"magicdb/mapi"
	"math"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/uopensail/ulib/prome"
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
	var features []engine.Fields
	if len(in.GetTables()) == 0 {
		features = srv.dbEngine.GetAll(key)
	} else {
		features = srv.dbEngine.Get(key, in.GetTables())
	}
	if len(features) <= 0 {
		stat.MarkErr()
		response.Msg = "not hit"
		response.Code = 404
		return response, nil
	}
	response.Features = make([]*mapi.Fields, len(features))
	for i := 0; i < len(features); i++ {
		response.Features[i] = &mapi.Fields{
			Column:     features[i].Column,
			Table:      features[i].TableName,
			FieldValue: features[i].FieldsValue,
		}
	}

	return response, nil

}

type StatusResponse struct {
	Code int32  `json:"code"`
	Msg  string `json:"msg"`
}

type FieldValue struct {
	StringV  string  `json:"string,omitempty"`
	Float32V float32 `json:"float32,omitempty"`
	Int64V   int64   `json:"int64,omitempty"`
}
type Field struct {
	Type  int8       `json:"type"`
	Name  string     `json:"name"`
	Value FieldValue `json:"value"`
}

type Fields struct {
	Table  string  `json:"table"`
	Column []Field `json:"column"`
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
	var features []engine.Fields
	if len(postData.GetTables()) == 0 {
		features = srv.dbEngine.GetAll(postData.Key)
	} else {
		features = srv.dbEngine.Get(postData.Key, postData.GetTables())
	}

	fieldsList := make([]Fields, len(features))
	for i := 0; i < len(features); i++ {
		fields := features[i]
		fieldsList[i].Table = fields.TableName
		fieldsList[i].Column = make([]Field, len(fields.Column))
		j := 0
		table.FieldValueReader(fields.FieldsValue, func(ft table.FieldType, v []byte) {
			fieldsList[i].Column[j].Name = fields.Column[j]
			fieldsList[i].Column[j].Type = int8(ft)
			switch ft {
			case table.Int64FieldType:
				rv := int64(binary.LittleEndian.Uint64(v))
				fieldsList[i].Column[j].Value.Int64V = rv
			case table.Float32FieldType:
				u := binary.LittleEndian.Uint32(v)
				rv := math.Float32frombits(u)
				fieldsList[i].Column[j].Value.Float32V = rv
			case table.BytesFieldType:
				rv := v
				fieldsList[i].Column[j].Value.StringV = string(rv)
			case table.NoneFieldType:
			}
			j++
		})
	}
	gCtx.JSON(http.StatusOK, fieldsList)
	return
}
