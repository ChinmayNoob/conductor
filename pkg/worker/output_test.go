package worker

import (
	"strings"
	"testing"
)

func TestCappedBufferKeepsSmallOutput(t *testing.T) {
	b := newCappedBuffer(100)
	b.Write([]byte("hello "))
	b.Write([]byte("world"))
	if got := b.String(); got != "hello world" {
		t.Errorf("got %q", got)
	}
}

func TestCappedBufferKeepsHeadAndTail(t *testing.T) {
	b := newCappedBuffer(10)
	for _, chunk := range []string{"ABCDE", "xxxxxxxxxx", "xxx", "VWXYZ"} {
		b.Write([]byte(chunk))
	}
	got := b.String()
	if !strings.HasPrefix(got, "ABCDE") || !strings.HasSuffix(got, "VWXYZ") {
		t.Errorf("want head ABCDE and tail VWXYZ, got %q", got)
	}
	if !strings.Contains(got, "[13 bytes truncated]") {
		t.Errorf("want truncation marker for 13 bytes, got %q", got)
	}
}

func TestCappedBufferReadFrom(t *testing.T) {
	b := newCappedBuffer(10) // keeps 5 bytes of head and 5 of tail
	b.Write([]byte("abc"))
	got, next := b.ReadFrom(0)
	if string(got) != "abc" || next != 3 {
		t.Fatalf("ReadFrom(0) = %q, %d", got, next)
	}
	b.Write([]byte("de"))
	if got, next = b.ReadFrom(next); string(got) != "de" || next != 5 {
		t.Fatalf("after more output: %q, %d", got, next)
	}
	if got, _ = b.ReadFrom(next); len(got) != 0 {
		t.Fatalf("nothing new, got %q", got)
	}

	// A reader that falls behind a large output skips the dropped middle.
	b.Write([]byte("0123456789XYZ")) // 18 bytes total; the tail keeps "89XYZ"
	got, next = b.ReadFrom(5)
	if next != 18 || !strings.Contains(string(got), "bytes truncated") || !strings.HasSuffix(string(got), "89XYZ") {
		t.Fatalf("lagging reader got %q, next %d", got, next)
	}
}
