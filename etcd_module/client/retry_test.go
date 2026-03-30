package client

import (
	"testing"
	"time"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestComputeRetryDelay(t *testing.T) {
	base := time.Second

	if got := computeRetryDelay(pb.BackoffType_BACKOFF_FIXED, 2, base); got != time.Second {
		t.Fatalf("fixed backoff mismatch: %v", got)
	}
	if got := computeRetryDelay(pb.BackoffType_BACKOFF_LINEAR, 3, base); got != 3*time.Second {
		t.Fatalf("linear backoff mismatch: %v", got)
	}
	if got := computeRetryDelay(pb.BackoffType_BACKOFF_EXPONENTIAL, 3, base); got != 8*time.Second {
		t.Fatalf("exponential backoff mismatch: %v", got)
	}
}
