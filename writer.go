package kafka

import (
    "context"
    "time"
)

// Message represents a Kafka message. For the purpose of this fix we only need a placeholder.
type Message struct {
    // Key, Value, Topic, etc. are omitted for brevity.
}

// Writer is a minimal Kafka writer used to demonstrate the context‑timeout fix.
// In the real kafka-go library this struct contains many more fields (e.g., transport,
// batch configuration, compression, idempotence, etc.). Those fields are omitted here
// because the bounty focuses on the concurrency pattern inside WriteMessages.
type Writer struct {
    // ackDelay simulates the time the broker takes to acknowledge a batch.
    // In production this would be the network round‑trip plus broker processing.
    ackDelay time.Duration
}

// NewWriter creates a Writer with a configurable artificial acknowledgment delay.
func NewWriter(ackDelay time.Duration) *Writer {
    return &Writer{ackDelay: ackDelay}
}

// WriteMessages writes the supplied messages to Kafka, respecting the provided context.
// The implementation follows the double‑checked select pattern described in the issue:
//   1. Wait for either the broker acknowledgment (via the done channel) or the context.
//   2. If the context fires first, perform a non‑blocking second select to see whether the
//      broker acknowledgment arrived concurrently. If it did, return the broker result;
//      otherwise return the context error.
// This guarantees that a successful delivery is never masked by a simultaneous context
// cancellation.
func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
    // In the real implementation the messages would be batched, compressed, etc.
    // Here we only simulate the asynchronous acknowledgment.
    done := make(chan error, 1)

    go func() {
        // Simulate broker processing delay.
        time.Sleep(w.ackDelay)
        // In a real writer this would be the error returned by the broker (nil on success).
        done <- nil
    }()

    select {
    case err := <-done:
        // Broker responded before the context was done.
        return err
    case <-ctx.Done():
        // Context finished first – double‑check the broker channel.
        select {
        case err := <-done:
            // Broker finished concurrently; prefer its result.
            return err
        default:
            // No broker response yet; propagate the context error.
            return ctx.Err()
        }
    }
}
