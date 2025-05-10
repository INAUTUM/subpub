package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"subpub/internal/server"
	pb "subpub/proto"
	"subpub/subpub"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupGRPCTest(t *testing.T) (pb.PubSubClient, func()) {
    logger, _ := zap.NewDevelopment()
    bus := subpub.NewSubPub(logger)
    
    lis := bufconn.Listen(1024 * 1024)
    srv := grpc.NewServer()
    pb.RegisterPubSubServer(srv, server.NewService(bus, logger))
    
    go func() {
        if err := srv.Serve(lis); err != nil {
            logger.Fatal("gRPC server failed", zap.Error(err))
        }
    }()

    conn, err := grpc.DialContext(context.Background(), "bufnet",
        grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
            return lis.Dial()
        }),
        grpc.WithTransportCredentials(insecure.NewCredentials()),
    )
    require.NoError(t, err)

    return pb.NewPubSubClient(conn), func() {
        conn.Close()
        srv.Stop()
        bus.Close(context.Background())
    }
}

func TestGRPCSubscribePublish(t *testing.T) {
    client, cleanup := setupGRPCTest(t)
    defer cleanup()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: "test"})
    require.NoError(t, err)

    time.Sleep(100 * time.Millisecond)

    _, err = client.Publish(ctx, &pb.PublishRequest{
        Key:  "test",
        Data: "message",
    })
    require.NoError(t, err)

    _, recvCancel := context.WithTimeout(ctx, 1*time.Second)
    defer recvCancel()
    
    msg, err := stream.Recv()
    require.NoError(t, err)
    require.Equal(t, "message", msg.Data)
}
func TestGRPCConcurrentSubscriptions(t *testing.T) {
    client, cleanup := setupGRPCTest(t)
    defer cleanup()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    msgChan := make(chan string, 3)
    
    for i := 0; i < 3; i++ {
        go func() {
            stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: "multi"})
            require.NoError(t, err)
            
            msg, err := stream.Recv()
            if err != nil {
                t.Errorf("Stream error: %v", err)
                return
            }
            msgChan <- msg.Data
        }()
    }

    time.Sleep(500 * time.Millisecond)

    _, err := client.Publish(ctx, &pb.PublishRequest{
        Key:  "multi",
        Data: "broadcast",
    })
    require.NoError(t, err)

    for i := 0; i < 3; i++ {
        select {
        case data := <-msgChan:
            require.Equal(t, "broadcast", data)
        case <-time.After(1 * time.Second):
            t.Fatal("Timeout waiting for message")
        }
    }
}