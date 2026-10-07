package worker

import (
	"fmt"
	"sync"
)

// cappedBuffer stores at most limit bytes of output. When output is larger it
// keeps the beginning and the end, since errors usually show up at the end.
type cappedBuffer struct {
	mu      sync.Mutex
	limit   int
	head    []byte
	tail    []byte // ring buffer of the most recent bytes
	tailPos int
	total   int64
}

func newCappedBuffer(limit int) *cappedBuffer {
	if limit <= 0 {
		limit = 1 << 20
	}
	return &cappedBuffer{limit: limit}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(p)
	b.total += int64(n)
	headCap := b.limit / 2
	if room := headCap - len(b.head); room > 0 {
		k := min(room, len(p))
		b.head = append(b.head, p[:k]...)
		p = p[k:]
	}
	tailCap := b.limit - headCap
	for _, c := range p {
		if len(b.tail) < tailCap {
			b.tail = append(b.tail, c)
		} else {
			b.tail[b.tailPos] = c
			b.tailPos = (b.tailPos + 1) % tailCap
		}
	}
	return n, nil
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	tail := append(append([]byte{}, b.tail[b.tailPos:]...), b.tail[:b.tailPos]...)
	kept := int64(len(b.head) + len(tail))
	if b.total == kept {
		return string(b.head) + string(tail)
	}
	return fmt.Sprintf("%s\n... [%d bytes truncated] ...\n%s", b.head, b.total-kept, tail)
}
