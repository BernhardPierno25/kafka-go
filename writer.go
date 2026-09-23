package kafka

import (
    "context"
)

// Message represents a Kafka message. The real implementation contains
// fields such as Topic, Partition, Key, Value, etc. For the purpose of this
// fix we only need the type to exist.
type Message struct{}

// Writer is a simplified version of the kafka-go Writer. In the actual
// library it handles batching, compression, idempotent writes, etc. Here we
// expose only the fields required to demonstrate the corrected select logic.
type Writer struct {
    // done is a channel that receives the result of a batch write. The
    // channel is closed or a value is sent once the broker has responded.
    // In production this would be a promise or a more complex struct.
    done chan error
}

// NewWriter creates a Writer with an initialized done channel.
func NewWriter() *Writer {
    return &Writer{done: make(chan error, 1)}
}

// SimulateCompletion is a helper used by tests to inject the broker's
// response into the Writer. It sends the provided error (or nil) on the done
// channel without blocking.
func (w *Writer) SimulateCompletion(err error) {
    select {
    case w.done <- err:
    default:
        // If the channel already has a value we leave it untouched – this
        // mirrors the behaviour of the real writer where only the first
        // result matters.
    }
}

// WriteMessages writes the supplied messages to Kafka. The implementation
// mirrors the pattern described in the issue: it prefers a successful broker
// acknowledgment over a context cancellation that occurs at the same time.
func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
    // In the real writer the messages would be queued, possibly batched, and
    // sent to the broker asynchronously. For this fix we focus solely on the
    // race‑condition handling between the completion channel and ctx.Done().
    select {
    case err := <-w.done:
        // Broker responded first – return its result (nil on success).
        return err
    case <-ctx.Done():
        // Context finished first; double‑check if the broker response arrived
        // concurrently. If it did, prefer that result.
        select {
        case err := <-w.done:
            return err
        default:
            return ctx.Err()
        }
    }
}
