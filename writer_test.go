package kafka

import (
    "context"
    "errors"
    "testing"
    "time"
)

// mockTransport implements the minimal subset of the transport interface used
// by Writer. It allows us to control when a batch is considered "delivered".
type mockTransport struct {
    // deliver is a channel that the test will close to signal a successful
    // write.
    deliver chan struct{}
}

func (m *mockTransport) WriteBatch(_ context.Context, _ *writeBatch) error {
    <-m.deliver // block until the test signals delivery
    return nil
}

func (m *mockTransport) Close() error { return nil }

func TestWriterWriteMessagesContextRace(t *testing.T) {
    // Create a writer that uses the mock transport.
    mt := &mockTransport{deliver: make(chan struct{})}
    w := &Writer{
        // Minimal fields required for the test; other fields use zero values.
        transport: mt,
        // The batch channel is unbuffered to force the writer to wait.
        batchc: make(chan *writeBatch),
    }

    // Start the writer's background goroutine (simplified for the test).
    go func() {
        for batch := range w.batchc {
            // Simulate the normal write path.
            err := w.writeBatch(context.Background(), batch)
            // Signal the result.
            batch.done <- err
            close(batch.done)
        }
    }()

    // Prepare a context that will be cancelled almost immediately.
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Fire the write in a separate goroutine.
    errCh := make(chan error, 1)
    go func() {
        errCh <- w.WriteMessages(ctx, Message{Value: []byte("test")})
    }()

    // Give the writer a tiny amount of time to start waiting on the batch.
    time.Sleep(10 * time.Millisecond)

    // Cancel the context *and* deliver the batch at the same time.
    close(mt.deliver) // unblock the mock transport – batch succeeds
    cancel()           // fire the context cancellation

    // Wait for the result.
    select {
    case err := <-errCh:
        if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
            t.Fatalf("expected nil or broker error, got %v", err)
        }
        // Success – the writer returned the broker result (nil).
    case <-time.After(2 * time.Second):
        t.Fatalf("WriteMessages did not return in time")
    }
}
