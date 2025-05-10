package subpub_test

import (
	"context"
	"fmt"
	"subpub/subpub"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestMain(m *testing.M) {
  	goleak.VerifyTestMain(m)
}

func TestSubscribePublish(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var (
        received string
        mu       sync.Mutex
        wg       sync.WaitGroup
    )
    wg.Add(1)

    sub, err := sp.Subscribe("test", func(msg interface{}) {
        defer wg.Done()
        mu.Lock()
        defer mu.Unlock()
        received = msg.(string)
    })
    assert.NoError(t, err)
    
    err = sp.Publish("test", "hello")
    assert.NoError(t, err)
    
    wg.Wait()
    mu.Lock()
    actual := received
    mu.Unlock()
    assert.Equal(t, "hello", actual)
    
    sub.Unsubscribe()
    sp.Close(context.Background())
}

func TestUnsubscribe(t *testing.T) {
	defer goleak.VerifyNone(t)
	
	logger, _ := zap.NewDevelopment()
	sp := subpub.NewSubPub(logger)
	callCount := 0
	
	sub, _ := sp.Subscribe("test", func(msg interface{}) {
		callCount++
	})
	
	sub.Unsubscribe()
	sp.Publish("test", "msg")
	
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, callCount)
	sp.Close(context.Background())
}

func TestSlowSubscriber(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var (
        callCount int
        mu        sync.Mutex
        wg        sync.WaitGroup
    )
    wg.Add(2)

    // быстрый подписчик
    sp.Subscribe("test", func(msg interface{}) {
        defer wg.Done()
        mu.Lock()
        callCount++
        mu.Unlock()
    })

    // медленный подписчик
    sp.Subscribe("test", func(msg interface{}) {
        defer wg.Done()
        time.Sleep(100 * time.Millisecond)
        mu.Lock()
        callCount++
        mu.Unlock()
    })

    sp.Publish("test", "msg")
    
    wg.Wait()
    mu.Lock()
    assert.Equal(t, 2, callCount)
    mu.Unlock()
    
    sp.Close(context.Background())
}

func TestCloseWithTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)
	
	logger, _ := zap.NewDevelopment()
	sp := subpub.NewSubPub(logger)
	var wg sync.WaitGroup
	wg.Add(1)

	sp.Subscribe("test", func(msg interface{}) {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond)
	})

	sp.Publish("test", "msg")
	
	ctx, cancel := context.WithTimeout(context.Background(), 50 * time.Millisecond)
	defer cancel()
	
	err := sp.Close(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	
	wg.Wait()
}

func TestFIFOOrder(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var (
        mu       sync.Mutex
        received []int
        wg       sync.WaitGroup
    )
    
    sub, _ := sp.Subscribe("order", func(msg interface{}) {
        mu.Lock()
        defer mu.Unlock()
        received = append(received, msg.(int))
        wg.Done()
    })

    const numMessages = 1000
    wg.Add(numMessages)
    
    // отправка сообщений в порядке 0 – 999
    for i := 0; i < numMessages; i++ {
        sp.Publish("order", i)
    }
    
    wg.Wait()
    
    mu.Lock()
    defer mu.Unlock()
    assert.Equal(t, numMessages, len(received))
    for i := 0; i < numMessages; i++ {
        assert.Equal(t, i, received[i])
    }
    
    sub.Unsubscribe()
    sp.Close(context.Background())
}

func TestPanicRecovery(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var (
        wg       sync.WaitGroup
        received int32
    )
    
    // подписчик с паникой
    wg.Add(1)
    sp.Subscribe("panic", func(msg interface{}) {
        defer wg.Done()
        panic("intentional panic")
    })

    // обычный подписчик
    sp.Subscribe("panic", func(msg interface{}) {
        atomic.AddInt32(&received, 1)
    })
    
    sp.Publish("panic", "test")
    wg.Wait()
    
    // время для обработки второго подписчика
    assert.Eventually(t, func() bool {
        return atomic.LoadInt32(&received) == 1
    }, 100*time.Millisecond, 5*time.Millisecond)
    
    sp.Close(context.Background())
}

func TestLoad(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var (
        counter int32
        wg      sync.WaitGroup
    )
    
    const (
        numSubscribers = 10
        numMessages    = 10000
    )
    
    // создаем подписчиков
    for i := 0; i < numSubscribers; i++ {
        sp.Subscribe("load", func(msg interface{}) {
            atomic.AddInt32(&counter, 1)
            wg.Done()
        })
    }
    
    wg.Add(numMessages * numSubscribers)
    
    // публикуем сообщения
    start := time.Now()
    for i := 0; i < numMessages; i++ {
        sp.Publish("load", i)
    }
    
    wg.Wait()
    elapsed := time.Since(start)
    
    t.Logf("Обработано %d сообщений за %s (%.0f msg/sec)", 
        numMessages*numSubscribers, 
        elapsed,
        float64(numMessages*numSubscribers)/elapsed.Seconds())
    
    assert.Equal(t, int32(numMessages*numSubscribers), counter)
    
    sp.Close(context.Background())
}

func TestMetrics(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    
    var wg sync.WaitGroup
    wg.Add(2)

    sub, err := sp.Subscribe("metrics", func(msg interface{}) {
        defer wg.Done()
        time.Sleep(10 * time.Millisecond)
    })
    require.NoError(t, err)
    defer sub.Unsubscribe()

    sp.Publish("metrics", "test1")
    sp.Publish("metrics", "test2")

    wg.Wait()

    metrics := sp.GetMetrics()
    assert.Equal(t, int64(2), metrics.MessagesSent, "Sent messages mismatch")
    assert.Equal(t, int64(2), metrics.MessagesHandled, "Handled messages mismatch")
    assert.True(t, 
        metrics.ProcessingTime >= 20*time.Millisecond,
        "Expected >=20ms, got %s", 
        metrics.ProcessingTime,
    )
    
    sp.Close(context.Background())
}

func TestCloseTwice(t *testing.T) {
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    sp.Close(context.Background())
    err := sp.Close(context.Background())
    assert.ErrorContains(t, err, "already closed")
}

func TestPublishAfterClose(t *testing.T) {
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    sp.Close(context.Background())
    err := sp.Publish("test", "data")
    assert.ErrorContains(t, err, "closed bus")
}

func TestQueueOverflow(t *testing.T) {
    core, logObserver := observer.New(zap.DebugLevel)
    logger := zap.New(core)
    sp := subpub.NewSubPub(logger)
    
    var wg sync.WaitGroup
    wg.Add(1)
    
    sub, _ := sp.Subscribe("test", func(msg interface{}) {
        defer wg.Done()
        time.Sleep(100 * time.Millisecond)
    })
    
    sp.Publish("test", "trigger")
    wg.Wait()
    
    for i := 0; i < 1000; i++ {
        sp.Publish("test", i)
    }
    
    var found bool
    for _, entry := range logObserver.All() {
        if entry.Message == "Signal channel overflow" {
            found = true
            break
        }
    }
    assert.True(t, found)
    
    sub.Unsubscribe()
    
    ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
    defer cancel()
    sp.Close(ctx)
}

func TestConcurrentPublishSubscribe(t *testing.T) {
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    var wg sync.WaitGroup
    
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                sp.Publish("data", fmt.Sprintf("msg-%d-%d", id, j))
            }
        }(i)
    }
    
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            sp.Subscribe("data", func(msg interface{}) {})
        }()
    }
    
    wg.Wait()
    sp.Close(context.Background())
}

func TestLoggingOnShutdown(t *testing.T) {
    core, logObserver := observer.New(zap.DebugLevel)
    logger := zap.New(core)
    sp := subpub.NewSubPub(logger)

    var wg sync.WaitGroup
    wg.Add(1)
    
    sp.Subscribe("log-test", func(msg interface{}) {
        defer wg.Done()
        time.Sleep(500 * time.Millisecond)
    })
    
    sp.Publish("log-test", "block")
    
    time.Sleep(50 * time.Millisecond)
    
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
    defer cancel()
    
    err := sp.Close(ctx)
    assert.ErrorIs(t, err, context.DeadlineExceeded)
    
    time.Sleep(100 * time.Millisecond)
    
    assert.Greater(t, 
        len(logObserver.FilterMessageSnippet("shutdown interrupted").All()), 
        0,
        "Should log shutdown interruption",
    )
}

func TestNilHandler(t *testing.T) {
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    _, err := sp.Subscribe("test", nil)
    assert.ErrorContains(t, err, "nil handler")
}

func TestEmptySubject(t *testing.T) {
    logger, _ := zap.NewDevelopment()
    sp := subpub.NewSubPub(logger)
    _, err := sp.Subscribe("", func(msg interface{}){})
    assert.ErrorContains(t, err, "invalid subject")
    
    err = sp.Publish("", "data")
    assert.ErrorContains(t, err, "invalid subject")
}