package coordclient

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fake is a coordinator that is either the leader or a standby pointing at
// the leader.
type fake struct {
	grpcapi.UnimplementedCoordinatorServiceServer
	mu     sync.Mutex
	leader string // "" = this one leads
	calls  atomic.Int32
}

func (f *fake) SubmitTask(context.Context, *grpcapi.ClientTaskRequest) (*grpcapi.ClientTaskResponse, error) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leader != "" {
		return nil, NotLeaderError(f.leader)
	}
	return &grpcapi.ClientTaskResponse{TaskId: "ok", Created: true}, nil
}

func (f *fake) setLeader(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leader = addr
}

func serve(t *testing.T, f *fake) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	grpcapi.RegisterCoordinatorServiceServer(s, f)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String(), s.Stop
}

func newClient(t *testing.T, seeds ...string) *Client {
	t.Helper()
	c, err := New(seeds, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func submit(t *testing.T, c *Client, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := c.SubmitTask(ctx, &grpcapi.ClientTaskRequest{})
	return err
}

func TestFollowsRedirectToLeader(t *testing.T) {
	leader := &fake{}
	leaderAddr, _ := serve(t, leader)
	standby := &fake{leader: leaderAddr}
	standbyAddr, _ := serve(t, standby)

	c := newClient(t, standbyAddr)
	if err := submit(t, c, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if c.Leader() != leaderAddr || leader.calls.Load() != 1 {
		t.Fatalf("leader = %q, leader calls = %d", c.Leader(), leader.calls.Load())
	}

	// Later calls go straight to the leader.
	before := standby.calls.Load()
	if err := submit(t, c, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if standby.calls.Load() != before {
		t.Fatal("the client asked the standby again instead of the known leader")
	}
}

func TestFailsOverWhenLeaderDies(t *testing.T) {
	a := &fake{}
	aAddr, stopA := serve(t, a)
	b := &fake{leader: aAddr}
	bAddr, _ := serve(t, b)

	c := newClient(t, aAddr, bAddr)
	if err := submit(t, c, 5*time.Second); err != nil || c.Leader() != aAddr {
		t.Fatalf("err=%v leader=%q", err, c.Leader())
	}

	// A dies; B wins the election a moment later.
	stopA()
	go func() {
		time.Sleep(300 * time.Millisecond)
		b.setLeader("")
	}()
	start := time.Now()
	if err := submit(t, c, 10*time.Second); err != nil {
		t.Fatalf("call failed during failover: %v", err)
	}
	if c.Leader() != bAddr {
		t.Fatalf("leader = %q, want %q", c.Leader(), bAddr)
	}
	t.Logf("failed over in %v", time.Since(start).Round(time.Millisecond))
}

func TestGivesUpWhenContextEnds(t *testing.T) {
	// A standby that points at a leader nobody can reach.
	addr, _ := serve(t, &fake{leader: "127.0.0.1:1"})

	c := newClient(t, addr)
	if err := submit(t, c, 500*time.Millisecond); err == nil {
		t.Fatal("expected an error with no reachable leader")
	}
}

func TestDoesNotRetryOtherErrors(t *testing.T) {
	f := &badRequest{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	grpcapi.RegisterCoordinatorServiceServer(s, f)
	go s.Serve(lis)
	defer s.Stop()

	c := newClient(t, lis.Addr().String())
	err = submit(t, c, 5*time.Second)
	if status.Code(err) != codes.InvalidArgument || f.calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d; want one call returning InvalidArgument", err, f.calls.Load())
	}
}

type badRequest struct {
	grpcapi.UnimplementedCoordinatorServiceServer
	calls atomic.Int32
}

func (b *badRequest) SubmitTask(context.Context, *grpcapi.ClientTaskRequest) (*grpcapi.ClientTaskResponse, error) {
	b.calls.Add(1)
	return nil, status.Error(codes.InvalidArgument, "bad task")
}
