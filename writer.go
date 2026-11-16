package kafka

import (
    "context"
    "errors"
    "sync"
    "time"
)

// ... other imports and code omitted for brevity ...

// writeBatch is the internal helper that sends a batch to the broker and
// signals its result on the batch's promise channel.
func (w *Writer) writeBatch(ctx context.Context, batch *writeBatch) {
    // existing implementation unchanged up to the point where the result
    // is sent on batch.done.
    // Assume batch.done is a chan error that will receive the result of the
    // broker write (nil on success).
    //
    // The original code waited like this:
    //
    //   select {
    //   case <-ctx.Done():
    //       return ctx.Err()
    //   case err := <-batch.done:
    //       return err
    //   }
    //
    // This caused a race where ctx.Done() could be chosen even if the batch
    // had already completed. The new implementation prefers the batch result.
    select {
    case err := <-batch.done:
        // Batch finished – return its result regardless of the context state.
        return err
    case <-ctx.Done():
        // Context finished first. It is possible that the batch also finished
        // at the same instant. Perform a non‑blocking check to avoid masking a
        // successful write.
        select {
        case err := <-batch.done:
            // The batch completed concurrently; return its result.
            return err
        default:
            // No result yet – propagate the context error.
            return ctx.Err()
        }
    }
}

// WriteMessages writes one or more messages to the topic. It batches messages
// internally and returns the first error encountered. The function now ensures
// that a successful broker acknowledgement is not overwritten by a context
// timeout that occurs at the same moment.
func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
    // Existing validation and batching logic unchanged.
    // ...
    // When the caller needs to wait for the batch to finish we now use the
    // revised waiting logic implemented in writeBatch.
    // For illustration, the simplified waiting path looks like this:
    batch := w.enqueue(msgs)
    // The batch's promise channel is created by enqueue and will be closed
    // with the result of the write.
    select {
    case err := <-batch.done:
        return err
    case <-ctx.Done():
        // Double‑check for a race condition.
        select {
        case err := <-batch.done:
            return err
        default:
            return ctx.Err()
        }
    }
}

// ... rest of the file unchanged ...
