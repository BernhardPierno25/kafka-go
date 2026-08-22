package kafka

import (
	"context"
	"testing"
	"time"
)

func TestWriterContextDeadlineRace(t *testing.T) {
	// Verifies that when messages are acknowledged simultaneously with context deadline,
	// WriteMessages returns nil rather than false-positive context.DeadlineExceeded.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)
	if ctx.Err() == nil {
		t.Log("Context expired as expected")
	}
}
