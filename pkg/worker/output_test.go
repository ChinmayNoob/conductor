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
