package main

import (
	"context"
	"errors"
)

// Writer demonstrates the fix for kafka-go Writer.WriteMessages context handling.
// Before: WriteMessages returned context timeout even when messages were successfully delivered to Kafka,
// causing false errors for callers. After: only return context error if delivery failed.

// MockWriter simulates the fixed behavior
type MockWriter struct {
	delivered bool
}

// WriteMessages fixed version — checks delivery before surfacing context error
func (w *MockWriter) WriteMessages(ctx context.Context, msgs ...string) error {
	// Simulate delivery
	w.delivered = true

	// Simulate context check AFTER delivery
	select {
	case <-ctx.Done():
		// If messages were already delivered, don't return context error
		if w.delivered {
			return nil // FIX: success takes precedence over cancelled context
		}
		return ctx.Err()
	default:
	}

	if len(msgs) == 0 {
		return errors.New("no messages")
	}
	return nil
}
