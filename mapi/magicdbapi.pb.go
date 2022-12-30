// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.4
// source: magicdbapi.proto

package mapi

import (
	context "context"
	sample "github.com/uopensail/ulib/sample"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Request struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key    string   `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Tables []string `protobuf:"bytes,2,rep,name=tables,proto3" json:"tables,omitempty"`
}

func (x *Request) Reset() {
	*x = Request{}
	if protoimpl.UnsafeEnabled {
		mi := &file_magicdbapi_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Request) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Request) ProtoMessage() {}

func (x *Request) ProtoReflect() protoreflect.Message {
	mi := &file_magicdbapi_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Request.ProtoReflect.Descriptor instead.
func (*Request) Descriptor() ([]byte, []int) {
	return file_magicdbapi_proto_rawDescGZIP(), []int{0}
}

func (x *Request) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Request) GetTables() []string {
	if x != nil {
		return x.Tables
	}
	return nil
}

type Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code     int32            `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Msg      string           `protobuf:"bytes,2,opt,name=msg,proto3" json:"msg,omitempty"`
	Features *sample.Features `protobuf:"bytes,3,opt,name=features,proto3" json:"features,omitempty"`
}

func (x *Response) Reset() {
	*x = Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_magicdbapi_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Response) ProtoMessage() {}

func (x *Response) ProtoReflect() protoreflect.Message {
	mi := &file_magicdbapi_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Response.ProtoReflect.Descriptor instead.
func (*Response) Descriptor() ([]byte, []int) {
	return file_magicdbapi_proto_rawDescGZIP(), []int{1}
}

func (x *Response) GetCode() int32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *Response) GetMsg() string {
	if x != nil {
		return x.Msg
	}
	return ""
}

func (x *Response) GetFeatures() *sample.Features {
	if x != nil {
		return x.Features
	}
	return nil
}

var File_magicdbapi_proto protoreflect.FileDescriptor

var file_magicdbapi_proto_rawDesc = []byte{
	0x0a, 0x10, 0x6d, 0x61, 0x67, 0x69, 0x63, 0x64, 0x62, 0x61, 0x70, 0x69, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x03, 0x61, 0x70, 0x69, 0x1a, 0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x75, 0x6f, 0x70, 0x65, 0x6e, 0x73, 0x61, 0x69, 0x6c, 0x2f, 0x75, 0x6c,
	0x69, 0x62, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72,
	0x65, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x33, 0x0a, 0x07, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x16, 0x0a, 0x06, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x22, 0x5e, 0x0a,
	0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x63, 0x6f, 0x64,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x10, 0x0a,
	0x03, 0x6d, 0x73, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6d, 0x73, 0x67, 0x12,
	0x2c, 0x0a, 0x08, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x10, 0x2e, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e, 0x46, 0x65, 0x61, 0x74, 0x75,
	0x72, 0x65, 0x73, 0x52, 0x08, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x32, 0x2f, 0x0a,
	0x07, 0x6d, 0x61, 0x67, 0x69, 0x63, 0x64, 0x62, 0x12, 0x24, 0x0a, 0x03, 0x47, 0x65, 0x74, 0x12,
	0x0c, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0d, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x08,
	0x5a, 0x06, 0x2e, 0x3b, 0x6d, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_magicdbapi_proto_rawDescOnce sync.Once
	file_magicdbapi_proto_rawDescData = file_magicdbapi_proto_rawDesc
)

func file_magicdbapi_proto_rawDescGZIP() []byte {
	file_magicdbapi_proto_rawDescOnce.Do(func() {
		file_magicdbapi_proto_rawDescData = protoimpl.X.CompressGZIP(file_magicdbapi_proto_rawDescData)
	})
	return file_magicdbapi_proto_rawDescData
}

var file_magicdbapi_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_magicdbapi_proto_goTypes = []interface{}{
	(*Request)(nil),         // 0: api.Request
	(*Response)(nil),        // 1: api.Response
	(*sample.Features)(nil), // 2: sample.Features
}
var file_magicdbapi_proto_depIdxs = []int32{
	2, // 0: api.Response.features:type_name -> sample.Features
	0, // 1: api.magicdb.Get:input_type -> api.Request
	1, // 2: api.magicdb.Get:output_type -> api.Response
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_magicdbapi_proto_init() }
func file_magicdbapi_proto_init() {
	if File_magicdbapi_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_magicdbapi_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Request); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_magicdbapi_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Response); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_magicdbapi_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_magicdbapi_proto_goTypes,
		DependencyIndexes: file_magicdbapi_proto_depIdxs,
		MessageInfos:      file_magicdbapi_proto_msgTypes,
	}.Build()
	File_magicdbapi_proto = out.File
	file_magicdbapi_proto_rawDesc = nil
	file_magicdbapi_proto_goTypes = nil
	file_magicdbapi_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// MagicdbClient is the client API for Magicdb service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type MagicdbClient interface {
	Get(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error)
}

type magicdbClient struct {
	cc grpc.ClientConnInterface
}

func NewMagicdbClient(cc grpc.ClientConnInterface) MagicdbClient {
	return &magicdbClient{cc}
}

func (c *magicdbClient) Get(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/api.magicdb/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MagicdbServer is the server API for Magicdb service.
type MagicdbServer interface {
	Get(context.Context, *Request) (*Response, error)
}

// UnimplementedMagicdbServer can be embedded to have forward compatible implementations.
type UnimplementedMagicdbServer struct {
}

func (*UnimplementedMagicdbServer) Get(context.Context, *Request) (*Response, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}

func RegisterMagicdbServer(s *grpc.Server, srv MagicdbServer) {
	s.RegisterService(&_Magicdb_serviceDesc, srv)
}

func _Magicdb_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MagicdbServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.magicdb/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MagicdbServer).Get(ctx, req.(*Request))
	}
	return interceptor(ctx, in, info, handler)
}

var _Magicdb_serviceDesc = grpc.ServiceDesc{
	ServiceName: "api.magicdb",
	HandlerType: (*MagicdbServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _Magicdb_Get_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "magicdbapi.proto",
}