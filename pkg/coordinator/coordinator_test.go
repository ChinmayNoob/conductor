package coordinator

import (
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/google/uuid"
)

// newTestServer builds a Server without a database or background loops.
func newTestServer(workerIDs ...uint32) *Server {
	s := &Server{
		workers:  make(map[uint32]*Worker),
		inFlight: make(map[uuid.UUID]dispatchedTask),
		wake:     make(chan struct{}, 1),
	}
	for _, id := range workerIDs {
		s.workers[id] = &Worker{
			ID:        id,
			IsHealthy: true,
			client:    grpcapi.NewWorkerServiceClient(nil),
		}
	}
	return s
}

func TestGetNextAvailableWorkerRoundRobin(t *testing.T) {
	s := newTestServer(30, 10, 20)

	var got []uint32
	for range 6 {
		got = append(got, s.getNextAvailableWorker().ID)
	}
	want := []uint32{10, 20, 30, 10, 20, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got order %v, want %v", got, want)
		}
	}
}

func TestGetNextAvailableWorkerSkipsBusyAndUnhealthy(t *testing.T) {
	s := newTestServer(1, 2, 3)
	s.workers[2].IsHealthy = false
	s.trackTask(uuid.New(), s.workers[1], time.Minute)

	for range 3 {
		if w := s.getNextAvailableWorker(); w.ID != 3 {
			t.Fatalf("got worker %d, want 3", w.ID)
		}
	}

	s.workers[3].IsHealthy = false
	if w := s.getNextAvailableWorker(); w != nil {
		t.Fatalf("got worker %d, want none", w.ID)
	}
}

func TestReleaseTaskFreesWorkerSlot(t *testing.T) {
	s := newTestServer(1)
	taskID := uuid.New()

	s.trackTask(taskID, s.workers[1], time.Minute)
	if w := s.getNextAvailableWorker(); w != nil {
		t.Fatal("worker should be busy while its task is in flight")
	}

	s.releaseTask(taskID)
	s.releaseTask(taskID) // releasing twice must not go negative
	if s.workers[1].inFlight != 0 {
		t.Fatalf("inFlight = %d, want 0", s.workers[1].inFlight)
	}
	if w := s.getNextAvailableWorker(); w == nil || w.ID != 1 {
		t.Fatal("worker should be available after its task is released")
	}
}
