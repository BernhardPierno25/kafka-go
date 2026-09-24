package kafka

import (
    "context"
    "testing"
    "time"
)

// TestWriteMessagesContextCanceledAfterAck verifies that WriteMessages returns nil
// when the broker acknowledges the batch even though the context is cancelled
// (or times out) at roughly the same moment.
func TestWriteMessagesContextCanceledAfterAck(t *testing.T) {
    // Simulate a fast broker acknowledgment.
    w := NewWriter(5 * time.Millisecond)

    // Context with a deadline slightly after the broker ack.
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    err := w.WriteMessages(ctx, Message{})
    if err != nil {
        t.Fatalf("expected nil error, got %v", err)
    }
}

// TestWriteMessagesContextTimeoutBeforeAck verifies that WriteMessages returns the
// context error when the broker does not acknowledge before the deadline.
func TestWriteMessagesContextTimeoutBeforeAck(t *testing.T) {
    // Simulate a slow broker acknowledgment.
    w := NewWriter(20 * time.Millisecond)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
    defer cancel()

    err := w.WriteMessages(ctx, Message{})
    if err == nil {
        t.Fatalf("expected context error, got nil")
    }
    if err != context.DeadlineExceeded && err != context.Canceled {
        t.Fatalf("expected context deadline/cancel error, got %v", err)
    }
}
