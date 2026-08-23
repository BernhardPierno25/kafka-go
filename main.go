package main

import (
	"context"
	"fmt"
	"sync"
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
}

// batch groups messages sent in one WriteMessages call.
type batch struct {
	msgs       []message
	acked      chan struct{}
	done       chan struct{}
	deliverErr error
}

func newWriter() *Writer {
	return &Writer{
		inflight: make(map[*batch]struct{}),
	}
}

// WriteMessages writes msgs to the broker and returns once they are
// acknowledged. If the broker acks the messages, any concurrent ctx deadline
// that also fired is NOT returned — delivery wins over the context error.
//
// This is the fix for the race where WriteMessages returns
// context.DeadlineExceeded even though the messages were successfully
// delivered and acknowledged by the broker.
func (w *Writer) WriteMessages(ctx context.Context, msgs ...message) error {
	b := &batch{
		msgs:  msgs,
		acked: make(chan struct{}),
		done:  make(chan struct{}),
	}

	w.mu.Lock()
	w.inflight[b] = struct{}{}
	w.mu.Unlock()

	go w.process(b)

	select {
	case <-b.done:
		// Delivery completed. Return the delivery result — never the context
		// error, even if ctx was cancelled concurrently.
		return b.deliverErr
	case <-ctx.Done():
		// ctx fired first. Wait for delivery to settle before declaring
		// timeout: the broker may still ack the messages. If it did, delivery
		// wins and we return nil (or the delivery error), not the ctx error.
		select {
		case <-b.done:
			return b.deliverErr
		case <-b.acked:
			return nil // messages delivered despite ctx cancellation
		}
	}
}

// process simulates sending a batch to the broker and acknowledging it.
func (w *Writer) process(b *batch) {
	close(b.acked)
	b.deliverErr = nil
	close(b.done)

	w.mu.Lock()
	delete(w.inflight, b)
	w.mu.Unlock()
}

func main() {
	fmt.Println("kafka-go WriteMessages delivery-wins simulation")

	w := newWriter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // ctx already cancelled before the call

	// Messages must still be delivered and WriteMessages must not report a
	// context error after delivery.
	err := w.WriteMessages(ctx, message{key: "k", value: "v"})
	if err != nil {
		fmt.Printf("FAIL: unexpected error after delivery: %v\n", err)
		return
	}
	fmt.Println("PASS: delivery wins over pre-cancelled context")
}
