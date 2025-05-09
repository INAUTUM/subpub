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
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
  	goleak.VerifyTestMain(m)
}

func TestSubscribePublish(t *testing.T) {
    defer goleak.VerifyNone(t)
    
    sp := subpub.NewSubPub()
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
	
	sp := subpub.NewSubPub()
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
    
    sp := subpub.NewSubPub()
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
	
	sp := subpub.NewSubPub()
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
    
    sp := subpub.NewSubPub()
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
    
    sp := subpub.NewSubPub()
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
    
    sp := subpub.NewSubPub()
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
    
    sp := subpub.NewSubPub()
    var (
        wg       sync.WaitGroup
        received int32
    )

    const numMessages = 2
    wg.Add(numMessages)

    sub, _ := sp.Subscribe("metrics", func(msg interface{}) {
        defer wg.Done()
        atomic.AddInt32(&received, 1)
        time.Sleep(10 * time.Millisecond)
    })
    defer sub.Unsubscribe()

    for i := 0; i < numMessages; i++ {
        sp.Publish("metrics", fmt.Sprintf("test%d", i+1))
    }

    wg.Wait()

    metrics := sp.GetMetrics()
    assert.Equal(t, int64(numMessages), metrics.MessagesSent, "Sent messages should match")
    assert.Equal(t, int64(numMessages), metrics.MessagesHandled, "Handled messages should match")
    assert.True(t, 
        time.Duration(metrics.ProcessingTime) >= numMessages*10*time.Millisecond,
        "Total processing time should be at least %v", 
        numMessages*10*time.Millisecond,
    )
    
    sp.Close(context.Background())
}