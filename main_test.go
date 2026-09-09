package main

import (
	"context"
	"testing"
	"time"
)

func TestWriteMessagesDeliveryWinsOverContextDeadline(t *testing.T) {
	writer := NewWriter()
	writer.BrokerAckDelay = 20 * time.Millisecond
	writer.GracePeriod = 40 * time.Millisecond

	// Context expires before broker delay, but within grace period
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := writer.WriteMessages(ctx, Message{Key: "testKey", Value: "testVal"})
	if err != nil {
		t.Fatalf("expected nil error (broker delivery prioritized), got: %v", err)
	}
}

func TestWriteMessagesContextTimeoutExceeded(t *testing.T) {
	writer := NewWriter()
	writer.BrokerAckDelay = 200 * time.Millisecond
	writer.GracePeriod = 20 * time.Millisecond

	// Context expires long before broker responds
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := writer.WriteMessages(ctx, Message{Key: "timeoutKey", Value: "timeoutVal"})
	if err == nil {
		t.Fatalf("expected context error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}

func TestWriteMessagesEmptyMessages(t *testing.T) {
	writer := NewWriter()
	ctx := context.Background()

	err := writer.WriteMessages(ctx)
	if err != nil {
		t.Fatalf("expected nil error for empty messages, got: %v", err)
	}
}
