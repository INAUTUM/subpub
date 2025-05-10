package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"subpub/internal/config"
	"subpub/internal/logging"
	"subpub/internal/server"
	pb "subpub/proto"
	"subpub/subpub"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func exitWithError(msg string) {
    fmt.Fprintln(os.Stderr, "ERROR:", msg)
    os.Exit(1)
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		exitWithError(fmt.Sprintf("Config error: %v", err))
	}

	logger, err := logging.NewLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logger: %v", err))
	}
	defer logger.Sync()

	logger.Info("Starting pubsub server",
		zap.Int("port", cfg.GRPCPort),
		zap.String("log_level", cfg.LogLevel))

	bus := subpub.NewSubPub(logger)
	defer func() {
		logger.Info("Shutting down pubsub bus")
		bus.Close(context.Background())
	}()

	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServer(grpcServer, server.NewService(bus, logger))
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	go func() {
		logger.Info("gRPC server listening", zap.String("addr", lis.Addr().String()))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")
	grpcServer.GracefulStop()
	logger.Info("Server stopped gracefully")
}