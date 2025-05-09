package subpub

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
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
	mu       sync.RWMutex
	subjects map[string][]*subscriber
	wg       sync.WaitGroup
	closed   bool
	metrics Metrics
}

// метрики
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
//

func NewSubPub() SubPub {
	return &subPub{
		subjects: make(map[string][]*subscriber),
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
    s.mu.Lock()
    defer s.mu.Unlock()

    if s.closed {
        return nil, errors.New("subpub closed")
    }

    sub := &subscriber{
        handler: cb,
        signal:  make(chan struct{}, 1),
        closeCh: make(chan struct{}),
        subPub:  s,
    }

    s.subjects[subject] = append(s.subjects[subject], sub)

    s.wg.Add(1)
    go sub.process()

    return &subscription{
        sub:     sub,
        subject: subject,
        s:       s,
    }, nil
}

func (s *subPub) Publish(subject string, msg interface{}) error {
    s.mu.RLock()
    defer s.mu.RUnlock()

    if s.closed {
        return errors.New("subpub closed")
    }

    s.metrics.mu.Lock()
    s.metrics.messagesSent++
    s.metrics.mu.Unlock()

    subs := s.subjects[subject]
    for _, sub := range subs {
        sub.enqueue(msg)
    }

    return nil
}

func (s *subPub) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("subpub already closed")
	}
	s.closed = true

	subsBySubject := s.subjects
	s.subjects = make(map[string][]*subscriber)
	s.mu.Unlock()

	var allSubs []*subscriber
	for _, subs := range subsBySubject {
		allSubs = append(allSubs, subs...)
	}

	for _, sub := range allSubs {
		sub.close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *subscriber) enqueue(msg interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.queue = append(s.queue, msg)

	select {
	case s.signal <- struct{}{}:
	default:
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
}

func (s *subscriber) process() {
    defer s.subPub.wg.Done()

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
        msg := s.queue[0]
        s.queue = s.queue[1:]

        func() {
            start := time.Now()
            s.mu.Unlock()
            defer s.mu.Lock()
            
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("Handler panic: %v", r)
                }
                
                elapsed := time.Since(start)
                s.subPub.metrics.mu.Lock()
                defer s.subPub.metrics.mu.Unlock()
                s.subPub.metrics.messagesHandled++
                s.subPub.metrics.processingTime += int64(elapsed)
            }()
            
            s.handler(msg)
        }()
    }
}