package server

import (
	"net"
	"strconv"

	"google.golang.org/grpc"
)

func NewGRPCServer() *grpc.Server {
    return grpc.NewServer()
}

func Start(grpcServer *grpc.Server, port int) (net.Listener, error) {
    lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
    if err != nil {
        return nil, err
    }
    
    go func() {
        if err := grpcServer.Serve(lis); err != nil {
            panic(err)
        }
    }()
    
    return lis, nil
}