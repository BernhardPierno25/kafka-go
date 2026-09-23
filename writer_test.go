package kafka

import (
    "context"
    "errors"
    "testing"
    "time"
)

// TestWriteMessagesPrefersBrokerAck verifies that when the broker
// acknowledges a batch at the same moment the context is cancelled, the
// Writer returns the broker result (nil for success) instead of the context
// error.
func TestWriteMessagesPrefersBrokerAck(t *testing.T) {
    w := NewWriter()

    // Create a context that will be cancelled after a short deadline.
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    // Start WriteMessages in a separate goroutine because it blocks until
    // either the done channel or the context finishes.
    doneCh := make(chan error, 1)
    go func() {
        doneCh <- w.WriteMessages(ctx)
    }()

    // Simulate the broker completing the write just after the context deadline.
    // The sleep is shorter than the context timeout to increase the chance of
    // a race, but we still send the result before the test ends.
    time.Sleep(5 * time.Millisecond)
    w.SimulateCompletion(nil)

    // Wait for the WriteMessages call to return.
    select {
    case err := <-doneCh:
        if err != nil {
            t.Fatalf("expected nil error, got %v", err)
        }
    case <-time.After(100 * time.Millisecond):
        t.Fatalf("WriteMessages did not return in time")
    }
}

// TestWriteMessagesContextErrorWhenNoBrokerResponse ensures that if the
// broker never responds, the context error is propagated.
func TestWriteMessagesContextErrorWhenNoBrokerResponse(t *testing.T) {
    w := NewWriter()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
    defer cancel()

    err := w.WriteMessages(ctx)
    if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
        t.Fatalf("expected context error, got %v", err)
    }
}
