package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrBatchTimeout indicates that the context expired before message delivery was acknowledged.
var ErrBatchTimeout = errors.New("kafka: batch delivery timed out")

// Message represents a record to be produced to Kafka.
type Message struct {
	Key   string
	Value string
}

// Batch represents a chunk of messages being dispatched to the broker.
type Batch struct {
	Messages []Message
	Done     chan struct{}
	Err      error
}

// Writer produces messages and handles batching & broker acknowledgments.
type Writer struct {
	mu             sync.Mutex
	inFlight       map[*Batch]struct{}
	BrokerAckDelay time.Duration
	GracePeriod    time.Duration
}

// NewWriter initializes a new Writer instance.
func NewWriter() *Writer {
	return &Writer{
		inFlight:       make(map[*Batch]struct{}),
		BrokerAckDelay: 20 * time.Millisecond,
		GracePeriod:    50 * time.Millisecond,
	}
}

// WriteMessages writes messages to the broker with race-free context completion.
// It prioritizes broker delivery acknowledgments over context.DeadlineExceeded / context.Canceled
// when delivery resolves concurrently with context expiration.
func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
	if len(msgs) == 0 {
		return nil
	}

	b := &Batch{
		Messages: msgs,
		Done:     make(chan struct{}),
	}

	w.mu.Lock()
	w.inFlight[b] = struct{}{}
	w.mu.Unlock()

	// Dispatch batch asynchronously to simulated broker
	go w.dispatchBatch(b)

	// Primary completion select
	select {
	case <-b.Done:
		// Delivery completed before context expiration
		return b.Err

	case <-ctx.Done():
		// Context expired or canceled. Perform a non-blocking check on b.Done
		// to resolve pseudo-random select ordering race conditions.
		select {
		case <-b.Done:
			// Broker already acknowledged delivery concurrently; prioritize delivery result
			return b.Err
		default:
		}

		// Allow a bounded grace period for in-flight broker network roundtrip to settle
		if w.GracePeriod > 0 {
			select {
			case <-b.Done:
				return b.Err
			case <-time.After(w.GracePeriod):
				return ctx.Err()
			}
		}

		return ctx.Err()
	}
}

// dispatchBatch simulates broker roundtrip and batch acknowledgment.
func (w *Writer) dispatchBatch(b *Batch) {
	if w.BrokerAckDelay > 0 {
		time.Sleep(w.BrokerAckDelay)
	}

	// Broker acknowledges successful write
	b.Err = nil
	close(b.Done)

	w.mu.Lock()
	delete(w.inFlight, b)
	w.mu.Unlock()
}

func main() {
	fmt.Println("=== kafka-go WriteMessages Delivery Race Resolution ===")

	writer := NewWriter()
	writer.BrokerAckDelay = 25 * time.Millisecond
	writer.GracePeriod = 50 * time.Millisecond

	// Test: Context with tight timeout (15ms) vs Broker Ack (25ms) within grace period
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()

	err := writer.WriteMessages(ctx, Message{Key: "user_123", Value: "event_signup"})
	if err != nil {
		fmt.Printf("❌ Failed: expected delivery to succeed within grace period, got error: %v\n", err)
	} else {
		fmt.Println("✅ Success: message acknowledged by broker took precedence over context timeout!")
	}
}
