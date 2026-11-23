package main

import "fmt"

func main() {
	fmt.Println("Hello, Bounty Hunter!")
}

// Assuming the WriteMessages function is defined here
func (w *Writer) WriteMessages(ctx context.Context, msgs ...Message) error {
    // Start a goroutine to handle the writing of messages
    done := make(chan error, 1)
    go func() {
        // Logic to write messages to Kafka
        // ...

        // Acknowledge messages after successful write
        done <- nil // or done <- someError if there's an error
    }()

    select {
    case err := <-done:
        // Only return the error if it occurred before the context was done
        if err != nil {
            return err
        }
        return nil
    case <-ctx.Done():
        // If context is done, we should not return context.DeadlineExceeded
        // if messages were already acknowledged
        return nil // or handle accordingly
    }
}