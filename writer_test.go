package main

import (
	"context"
	"testing"
	"time"
)

func TestWriteMessages_ContextTimeoutAfterDelivery(t *testing.T) {
	w := &MockWriter{}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	time.Sleep(2 * time.Nanosecond)
	defer cancel()

	// Messages were delivered, context timed out after — should not return timeout
	err := w.WriteMessages(ctx, "msg1", "msg2")
	if err != nil {
		t.Fatalf("expected nil when delivered despite timeout, got %v", err)
	}
}

func TestWriteMessages_NoMessages(t *testing.T) {
	w := &MockWriter{}
	err := w.WriteMessages(context.Background())
	if err == nil {
		t.Fatal("expected error for no messages")
	}
}
