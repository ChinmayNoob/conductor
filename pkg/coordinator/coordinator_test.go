package coordinator

import (
	"github.com/ChinmayNoob/conductor/pkg/db"
	"testing"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"github.com/google/uuid"
)

// newTestServer builds a Server without a database or background loops.
// Every worker gets one slot and the shell label.
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
			Slots:     1,
			Labels:    map[string]string{"type.shell": "true"},
			client:    grpcapi.NewWorkerServiceClient(nil),
		}
	}
	return s
}

var shell = map[string]string{"type.shell": "true"}

func TestChooseWorkerRoundRobin(t *testing.T) {
	s := newTestServer(30, 10, 20)

	var got []uint32
	for range 6 {
		got = append(got, s.chooseWorker(shell).ID)
	}
	want := []uint32{10, 20, 30, 10, 20, 30}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got order %v, want %v", got, want)
		}
	}
}

func TestChooseWorkerSkipsBusyUnhealthyAndDraining(t *testing.T) {
	s := newTestServer(1, 2, 3, 4)
	s.workers[2].IsHealthy = false
	s.workers[4].Draining = true
	s.trackTask(&db.Task{ID: uuid.New(), Attempt: 1, TimeoutSeconds: 60}, s.workers[1])

	for range 3 {
		if w := s.chooseWorker(shell); w.ID != 3 {
			t.Fatalf("got worker %d, want 3", w.ID)
		}
	}

	s.workers[3].IsHealthy = false
	if w := s.chooseWorker(shell); w != nil {
		t.Fatalf("got worker %d, want none", w.ID)
	}
}

func TestChooseWorkerUsesSlots(t *testing.T) {
	s := newTestServer(1)
	s.workers[1].Slots = 2

	s.trackTask(&db.Task{ID: uuid.New(), Attempt: 1, TimeoutSeconds: 60}, s.workers[1])
	if s.chooseWorker(shell) == nil {
		t.Fatal("worker with a free slot was not chosen")
	}
	s.trackTask(&db.Task{ID: uuid.New(), Attempt: 1, TimeoutSeconds: 60}, s.workers[1])
	if s.chooseWorker(shell) != nil {
		t.Fatal("worker with no free slot was chosen")
	}
}

func TestChooseWorkerMatchesLabels(t *testing.T) {
	s := newTestServer(1, 2)
	s.workers[2].Labels = map[string]string{"type.shell": "true", "gpu": "true"}

	for range 3 {
		if w := s.chooseWorker(map[string]string{"type.shell": "true", "gpu": "true"}); w == nil || w.ID != 2 {
			t.Fatalf("got %v, want the GPU worker", w)
		}
	}
	if w := s.chooseWorker(map[string]string{"type.container": "true"}); w != nil {
		t.Fatalf("got worker %d for a label nobody has", w.ID)
	}
}

func TestFreeCapacity(t *testing.T) {
	s := newTestServer(1, 2, 3)
	s.workers[3].Labels = map[string]string{"type.shell": "true", "region": "eu"}
	labels, free := s.freeCapacity()
	if len(labels) != 2 || free != 3 {
		t.Fatalf("got %d label sets and %d free slots, want 2 and 3", len(labels), free)
	}
}

func TestReleaseTaskFreesWorkerSlot(t *testing.T) {
	s := newTestServer(1)
	taskID := uuid.New()

	s.trackTask(&db.Task{ID: taskID, Attempt: 1, TimeoutSeconds: 60}, s.workers[1])
	if w := s.chooseWorker(shell); w != nil {
		t.Fatal("worker should be busy while its task is in flight")
	}

	s.releaseTask(taskID)
	s.releaseTask(taskID) // releasing twice must not go negative
	if s.workers[1].inFlight != 0 {
		t.Fatalf("inFlight = %d, want 0", s.workers[1].inFlight)
	}
	if w := s.chooseWorker(shell); w == nil || w.ID != 1 {
		t.Fatal("worker should be available after its task is released")
	}
}
