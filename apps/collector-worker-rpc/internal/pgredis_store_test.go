package internal

import (
	"testing"
	"time"
)

func TestNormalizeUsageMonthFallsBackToSeenMonthStart(t *testing.T) {
	seenAt := time.Date(2026, 4, 13, 12, 30, 0, 0, time.UTC)
	if got := normalizeUsageMonth("", seenAt); got != "2026-04-01" {
		t.Fatalf("expected usage month 2026-04-01, got %q", got)
	}
}

func TestNormalizeUsageMonthKeepsExplicitValue(t *testing.T) {
	if got := normalizeUsageMonth("2026-04-01", time.Now().UTC()); got != "2026-04-01" {
		t.Fatalf("expected explicit usage month to be preserved, got %q", got)
	}
}
