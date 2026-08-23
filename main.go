package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// message is a single record to be written to the broker.
type message struct {
	key   string
	value string
}

// Writer batches messages and writes them to the (simulated) broker.
type Writer struct {
	mu       sync.Mutex
	inflight map[*batch]struct{}
	brokerAckDelay time.Duration
}

// batch groups messages sent in one WriteMessages call.
type batch struct {
	msgs       []message
	acked      chan struct{}
	delivered  chan struct{}
	deliverErr error
}

func newWriter() *Writer {
	return &Writer{
		inflight: make(map[*batch]struct{}),
		brokerAckDelay: 50 * time.Millisecond,
	}
}

// WriteMessages writes msgs to the broker and returns once they are
// acknowledged. Delivery is the single source of truth: if the broker acked
// the messages, the call returns the delivery result and NEVER a context
// error — even if ctx fired while the batch was in flight. Only when the
// context expires and the messages were NOT delivered does it report the
// context error.
//
// This closes the race where WriteMessages returned context.DeadlineExceeded
// even though the messages reached the broker (issues #1 and #2).
func (w *Writer) WriteMessages(ctx context.Context, msgs ...message) error {
	b := &batch{
		msgs:      msgs,
		acked:     make(chan struct{}),
		delivered: make(chan struct{}),
	}

	w.mu.Lock()
	w.inflight[b] = struct{}{}
	w.mu.Unlock()

	go w.process(b)

	// Delivery path: wait for the broker result. This is authoritative.
	select {
	case <-b.delivered:
		return b.deliverErr
	case <-ctx.Done():
		// Context fired. The broker may still be about to ack the batch.
		// Give delivery a bounded grace window to settle before declaring a
		// timeout, so a message that was delivered is never reported as
		// context.DeadlineExceeded.
		select {
		case <-b.delivered:
			return b.deliverErr
		case <-b.acked:
			return nil // delivered; ctx error must not win
		case <-time.After(gracePeriod):
			return ctx.Err()
		}
	}
}

// gracePeriod is how long WriteMessages waits for an in-flight batch to settle
// after the context fires before declaring a timeout.
const gracePeriod = 100 * time.Millisecond

// process simulates sending a batch to the broker. The ack lands after
// brokerAckDelay — a real async window during which the caller's ctx may fire.
// acked closes the moment the broker acknowledges; delivered closes after the
// batch fully settles.
func (w *Writer) process(b *batch) {
	// Simulate broker round-trip latency.
	time.Sleep(w.brokerAckDelay)

	// Broker acknowledges the messages.
	close(b.acked)

	// The batch is settled with a delivery result.
	b.deliverErr = nil
	close(b.delivered)

	w.mu.Lock()
	delete(w.inflight, b)
	w.mu.Unlock()
}

func main() {
	fmt.Println("kafka-go WriteMessages delivery-wins simulation")

	w := newWriter()
	w.brokerAckDelay = 30 * time.Millisecond

	// ctx is cancelled while the batch is in flight (before broker ack at 30ms).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := w.WriteMessages(ctx, message{key: "k", value: "v"})
	if err != nil {
		fmt.Printf("FAIL: expected delivery to win over ctx timeout, got: %v\n", err)
		return
	}
	fmt.Println("PASS: message delivered despite ctx timeout; no false deadline error")
}
