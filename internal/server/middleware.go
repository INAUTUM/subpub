package server

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func LoggingInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
    return func(
        ctx context.Context,
        req interface{},
        info *grpc.UnaryServerInfo,
        handler grpc.UnaryHandler,
    ) (resp interface{}, err error) {
        start := time.Now()
        
        requestID := uuid.New().String()
        
        md, _ := metadata.FromIncomingContext(ctx)
        
        logger.Debug("Request started",
            zap.String("method", info.FullMethod),
            zap.String("request_id", requestID),
            zap.Any("metadata", md),
            zap.Any("request", req),
        )

        ctx = context.WithValue(ctx, "request_id", requestID)

        defer func() {
            duration := time.Since(start)
            
            logger.Info("Request completed",
                zap.String("method", info.FullMethod),
                zap.String("request_id", requestID),
                zap.Duration("duration", duration),
                zap.Error(err),
                zap.Any("response", resp),
            )
        }()

        return handler(ctx, req)
    }
}

func StreamLoggingInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
    return func(
        srv interface{},
        ss grpc.ServerStream,
        info *grpc.StreamServerInfo,
        handler grpc.StreamHandler,
    ) error {
        start := time.Now()
        requestID := uuid.New().String()

        logger.Debug("Stream started",
            zap.String("method", info.FullMethod),
            zap.String("request_id", requestID),
        )

        err := handler(srv, ss)
        
        logger.Info("Stream completed",
            zap.String("method", info.FullMethod),
            zap.String("request_id", requestID),
            zap.Duration("duration", time.Since(start)),
            zap.Error(err),
        )

        return err
    }
}