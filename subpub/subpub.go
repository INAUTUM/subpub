package subpub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// MessageHandler is a callback function that processes messages delivered to subscribers.
type MessageHandler func(msg interface{})

type Subscription interface {
	// Unsubscribe will remove interest in the current subject subscription is for.
	Unsubscribe()
}

type SubPub interface {
	// Subscribe creates an asynchronous queue subscriber on the given subject. 
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	// Publish publishes the msg argument to the given subject.

	Publish(subject string, msg interface{}) error

	// Close will shutdown sub-pub system.
	// May be blocked by data delivery until the context is canceled. 
	Close(ctx context.Context) error

	GetMetrics() MetricsSnapshot
}

type Metrics struct {
    messagesSent    int64
    messagesHandled int64
    processingTime  int64
    mu              sync.RWMutex
}

type MetricsSnapshot struct {
    MessagesSent    int64
    MessagesHandled int64
    ProcessingTime  time.Duration
}

type subscriber struct {
	handler  MessageHandler
	mu       sync.Mutex
	queue    []interface{}
	signal   chan struct{}
	closeCh  chan struct{}
	closed   bool
	subPub   *subPub
}

type subscription struct {
	sub     *subscriber
	subject string
	s       *subPub
}

type subPub struct {
    wgCounter int64
	mu       sync.RWMutex
	subjects map[string][]*subscriber
	wg       sync.WaitGroup
	closed   bool
	metrics Metrics
	logger   *zap.Logger
    log *zap.Logger
}

func (s *subPub) GetMetrics() MetricsSnapshot {
    s.metrics.mu.RLock()
    defer s.metrics.mu.RUnlock()
    
    return MetricsSnapshot{
        MessagesSent:    s.metrics.messagesSent,
        MessagesHandled: s.metrics.messagesHandled,
        ProcessingTime:  time.Duration(s.metrics.processingTime),
    }
}

func (s *subPub) updateMetrics(start time.Time) {
    s.metrics.mu.Lock()
    defer s.metrics.mu.Unlock()
    
    s.metrics.messagesHandled++
    s.metrics.processingTime += int64(time.Since(start))
}

func NewSubPub(logger *zap.Logger) SubPub {
    return &subPub{
        subjects: make(map[string][]*subscriber),
        logger:   logger,
    }
}

func (s *subscription) Unsubscribe() {
	s.s.removeSubscriber(s.subject, s.sub)
}

func (s *subPub) removeSubscriber(subject string, sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subs := s.subjects[subject]
	for i, candidate := range subs {
		if candidate == sub {
			subs = append(subs[:i], subs[i+1:]...)
			s.subjects[subject] = subs
			break
		}
	}
	sub.close()
}

func (s *subPub) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
    if subject == "" {
        return nil, errors.New("invalid subject")
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    if cb == nil {
        return nil, errors.New("nil handler")
    }

    if s.closed {
        s.logger.Warn("Attempt to subscribe to closed bus", zap.String("subject", subject))
        return nil, errors.New("subpub closed")
    }

    sub := &subscriber{
        signal:  make(chan struct{}, 100),
        handler: cb,
        closeCh: make(chan struct{}),
        subPub:  s,
    }

    s.subjects[subject] = append(s.subjects[subject], sub)
    s.logger.Debug("New subscription created", zap.String("subject", subject), zap.Int("total_subscribers", len(s.subjects[subject])))

    atomic.AddInt64(&s.wgCounter, 1)
    s.wg.Add(1)
    go sub.process()

    return &subscription{
        sub:     sub,
        subject: subject,
        s:       s,
    }, nil
}

func (s *subPub) Publish(subject string, msg interface{}) error {
    if subject == "" {
        return errors.New("invalid subject")
    }

    s.mu.RLock()
    defer s.mu.RUnlock()

    if s.closed {
        return errors.New("pubsub: cannot publish to closed bus")
    }

    subs := s.subjects[subject]
    
    s.metrics.mu.Lock()
    s.metrics.messagesSent += int64(len(subs))
    s.metrics.mu.Unlock()

    if s.logger != nil {
        s.logger.Info("Publishing message",
            zap.String("subject", subject),
            zap.Any("message", msg),
            zap.Int("subscribers", len(subs)),
        )
    }

    for _, sub := range subs {
        sub.enqueue(msg)
    }
    
    return nil
}

func (s *subPub) Close(ctx context.Context) error {
    start := time.Now()
    s.mu.Lock()
    
    if s.closed {
        s.mu.Unlock()
        s.logger.Warn("Attempt to close already closed pubsub")
        return errors.New("subpub already closed")
    }
    
    s.closed = true
    totalSubjects := len(s.subjects)
    totalSubscribers := 0
    
    subsBySubject := s.subjects
    var allSubs []*subscriber
    for _, subs := range subsBySubject {
        totalSubscribers += len(subs)
        allSubs = append(allSubs, subs...)
    }
    
    s.logger.Info("Starting pubsub shutdown",
        zap.Int("subjects", totalSubjects),
        zap.Int("total_subscribers", totalSubscribers),
    )
    
    s.subjects = make(map[string][]*subscriber)
    s.mu.Unlock()
    
    s.logger.Debug("Closing all subscribers",
        zap.Int("subscribers", totalSubscribers),
    )
    
    for i, sub := range allSubs {
        sub.close()
        if (i+1)%1000 == 0 {
            s.logger.Debug("Closed subscribers batch",
                zap.Int("count", i+1),
                zap.Int("total", totalSubscribers),
            )
        }
    }
    
    s.logger.Info("Waiting for handlers completion",
        zap.Int("pending_handlers", int(atomic.LoadInt64(&s.wgCounter))),
    )
    
    done := make(chan struct{})
    go func() {
        s.wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        s.logger.Info("Pubsub closed successfully",
            zap.Duration("shutdown_duration", time.Since(start)),
            zap.Int("subjects_closed", totalSubjects),
            zap.Int("subscribers_closed", totalSubscribers),
        )
        return nil
        
    case <-ctx.Done():
        // Force close all remaining subscribers
        for _, sub := range allSubs {
            sub.close()
        }
        
        // Wait again after force close
        select {
        case <-done:
        default:
            s.wg.Wait()
        }
        
        s.logger.Warn("Pubsub shutdown interrupted",
            zap.Duration("shutdown_duration", time.Since(start)),
            zap.String("reason", ctx.Err().Error()),
            zap.Int("subjects_closed", totalSubjects),
            zap.Int("subscribers_closed", totalSubscribers),
            zap.Int("pending_handlers", int(atomic.LoadInt64(&s.wgCounter))),
        )
        return ctx.Err()
    }
}

func (s *subscriber) enqueue(msg interface{}) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        s.subPub.logger.Warn("Enqueue to closed subscriber",
            zap.Any("message_type", fmt.Sprintf("%T", msg)),
            zap.Int("queue_size", len(s.queue)),
        )
        return
    }

    before := len(s.queue)
    s.queue = append(s.queue, msg)
    after := len(s.queue)

    logLevel := zap.DebugLevel
    if after > 1000 {
        logLevel = zap.WarnLevel
    }

    s.subPub.logger.Log(logLevel, "Message enqueued",
        zap.Int("queue_size_before", before),
        zap.Int("queue_size_after", after),
        zap.Any("message_type", fmt.Sprintf("%T", msg)),
        zap.Int("signal_channel_cap", cap(s.signal)),
        zap.Int("signal_channel_len", len(s.signal)),
    )

    select {
        case 
            s.signal <- struct{}{}:
        default:
            s.subPub.logger.Warn("Signal channel overflow",
                zap.Int("queue_size", after),
                zap.Int("channel_capacity", cap(s.signal)),
            )
    }
}

func (s *subscriber) close() {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if s.closed {
        return
    }
    
    s.closed = true
    close(s.closeCh)
    s.subPub.logger.Debug("Subscriber closed", 
        zap.Int("pending_messages", len(s.queue)))
}

func (s *subscriber) process() {
    defer func() {
        s.subPub.wg.Done()
        atomic.AddInt64(&s.subPub.wgCounter, -1)
        s.subPub.logger.Debug("Subscriber processor stopped")
    }()

    s.subPub.logger.Debug("Subscriber processor started")

    for {
        select {
        case <-s.signal:
            s.processQueue()
        case <-s.closeCh:
            s.processQueue()
            return
        }
    }
}

func (s *subscriber) processQueue() {
    s.mu.Lock()
    defer s.mu.Unlock()

    for len(s.queue) > 0 {
        select {
        case <-s.closeCh:
            return
        default:
            msg := s.queue[0]
            s.queue = s.queue[1:]

            start := time.Now()
            s.mu.Unlock()
            
            func() {
                defer func() {
                    if r := recover(); r != nil {
                        log.Printf("Handler panic: %v", r)
                    }
                }()
                s.handler(msg)
            }()
            
            s.mu.Lock()
            elapsed := time.Since(start)
            
            s.subPub.metrics.mu.Lock()
            s.subPub.metrics.messagesHandled++
            s.subPub.metrics.processingTime += int64(elapsed)
            s.subPub.metrics.mu.Unlock()
        }
    }
}