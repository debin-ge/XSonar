package shared

import (
	"math"
	"testing"
)

func TestInt32FromIntReturnsConvertedValue(t *testing.T) {
	value, err := Int32FromInt(12345)
	if err != nil {
		t.Fatalf("Int32FromInt returned error: %v", err)
	}
	if value != 12345 {
		t.Fatalf("expected 12345, got %d", value)
	}
}

func TestInt32FromIntRejectsOverflow(t *testing.T) {
	_, err := Int32FromInt(int(math.MaxInt32) + 1)
	if err == nil {
		t.Fatal("expected overflow to fail")
	}
}
