package server

import (
	"context"
	"fmt"
	"net"
	"time"

	pb "subpub/proto"

	"subpub/subpub"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type PubSubService struct {
    pb.UnimplementedPubSubServer
    bus   subpub.SubPub
    log   *zap.Logger
}

func NewService(bus subpub.SubPub, log *zap.Logger) *PubSubService {
    return &PubSubService{
        bus: bus,
        log: log.Named("grpc_service"),
    }
}

func getClientIP(ctx context.Context) string {
    p, ok := peer.FromContext(ctx)
    if !ok {
        return "unknown"
    }
    
    addr := p.Addr.String()
    host, _, err := net.SplitHostPort(addr)
    if err != nil {
        return addr
    }
    
    return host
}

func (s *PubSubService) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	ctx := stream.Context()
	s.log.Info("New subscription request",
		zap.String("key", req.Key),
		zap.String("client", getClientIP(ctx)))

	sub, err := s.bus.Subscribe(req.Key, func(msg interface{}) {
		s.log.Debug("Sending message to client",
			zap.String("key", req.Key),
			zap.Any("message", msg))

		if err := stream.Send(&pb.Event{Data: fmt.Sprintf("%v", msg)}); err != nil {
			s.log.Error("Failed to send message",
				zap.String("key", req.Key),
				zap.Error(err))
		}
	})

	if err != nil {
		s.log.Error("Subscription failed",
			zap.String("key", req.Key),
			zap.Error(err))
		return status.Error(codes.Internal, "subscription failed")
	}
	defer sub.Unsubscribe()

	<-ctx.Done()
	s.log.Info("Subscription terminated",
		zap.String("key", req.Key),
		zap.String("reason", ctx.Err().Error()))
	return nil
}

func (s *PubSubService) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
    const maxDataLogLength = 100 
    
    s.log.Debug("Publish request received",
        zap.String("key", req.Key),
        zap.String("data_snippet", truncateString(req.Data, maxDataLogLength)),
        zap.Int("data_length", len(req.Data)),
        zap.String("caller", getClientIP(ctx)),
    )

    if req.Key == "" || req.Data == "" {
        s.log.Warn("Invalid publish request",
            zap.String("validation_error", "empty key or data"),
        )
        return nil, status.Error(codes.InvalidArgument, "key and data required")
    }
    
    start := time.Now()
    err := s.bus.Publish(req.Key, req.Data)
    
    if err != nil {
        s.log.Error("Publish operation failed",
            zap.String("key", req.Key),
            zap.Duration("duration", time.Since(start)),
            zap.Error(err),
            zap.Stack("stack"), 
        )
        return nil, status.Error(codes.Internal, "publish failed")
    }

    s.log.Info("Message published successfully",
        zap.String("key", req.Key),
        zap.Duration("duration", time.Since(start)),
        zap.Int("data_length", len(req.Data)),
    )
    
    return &emptypb.Empty{}, nil
}

func truncateString(s string, maxLen int) string {
    if len(s) <= maxLen {
        return s
    }
    return s[:maxLen] + "..."
}