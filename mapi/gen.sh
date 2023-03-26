protoc -I. -I$GOPATH/src --go_out=:. --go-grpc_out=:. *.proto
