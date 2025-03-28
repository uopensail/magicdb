package services

import (
	"context"
	"magicdb/mapi"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/uopensail/ulib/prome"
)

// Get retrieves data from the database based on the given request.
// It returns a gRPC response and an error if any issues occur.
func (srv *Services) Get(ctx context.Context, in *mapi.Request) (*mapi.Response, error) {
	// Start performance monitoring
	stat := prome.NewStat("App.Get")
	defer stat.End()

	// Initialize response
	response := &mapi.Response{}
	key := in.GetKey()

	// Validate input
	if len(key) == 0 {
		stat.MarkErr()
		zap.L().Warn("Key is empty in request")
		response.Msg = "key is empty"
		response.Code = 400 // Bad request
		return response, nil
	}

	// Query the database
	if len(in.GetTables()) == 0 {
		response.Data = srv.db.GetAll(key)
	} else {
		response.Data = srv.db.Get(key, in.GetTables())
	}

	// Check if data was found
	if len(response.Data) == 0 {
		stat.MarkErr()
		zap.L().Info("Data not found", zap.String("key", key))
		response.Msg = "not hit"
		response.Code = 404 // Not found
		return response, nil
	}

	// Success
	zap.L().Info("Data retrieved successfully", zap.String("key", key))
	response.Code = 200 // Success
	response.Msg = "success"
	return response, nil
}

// StatusResponse defines a standard HTTP response format.
type StatusResponse struct {
	Code int32  `json:"code"` // Status code
	Msg  string `json:"msg"`  // Status message
}

// GetHandler is an HTTP handler for the "Get" operation.
// It processes client requests, calls the Get method, and returns the result as JSON.
func (srv *Services) GetHandler(gCtx *gin.Context) {
	// Start performance monitoring
	pStat := prome.NewStat("GetHandler")
	defer pStat.End()

	// Parse request body
	var postData mapi.Request
	if err := gCtx.ShouldBind(&postData); err != nil {
		zap.L().Error("Failed to bind request", zap.Error(err))
		gCtx.JSON(http.StatusBadRequest, StatusResponse{
			Code: 400, // Bad request
			Msg:  err.Error(),
		})
		return
	}

	// Call the Get method
	response, err := srv.Get(context.Background(), &postData)
	if err != nil {
		zap.L().Error("Error in Get method", zap.Error(err))
		gCtx.JSON(http.StatusInternalServerError, StatusResponse{
			Code: 500, // Internal server error
			Msg:  err.Error(),
		})
		return
	}

	// Return the response as JSON
	gCtx.JSON(http.StatusOK, response)
}
