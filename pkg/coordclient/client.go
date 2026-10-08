// Package coordclient is a coordinator client that follows the leader.
//
// Several coordinators may run; only the leader serves requests. Standbys
// answer with a FailedPrecondition error naming the leader, and this client
// redirects there. When the leader dies, calls fail with Unavailable; the
// client then cycles through the seed addresses, waiting out the election,
// until a new leader answers or the call's context expires.
package coordclient

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// notLeaderPrefix starts the message of a standby's rejection.
const notLeaderPrefix = "not the leader"

// NotLeaderError is what a standby coordinator returns. leader may be empty
// while an election is in progress.
func NotLeaderError(leader string) error {
	return status.Errorf(codes.FailedPrecondition, "%s; leader=%s", notLeaderPrefix, leader)
}

// leaderFrom extracts the leader address from a standby's rejection.
func leaderFrom(err error) (addr string, notLeader bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition || !strings.HasPrefix(st.Message(), notLeaderPrefix) {
		return "", false
	}
	_, addr, _ = strings.Cut(st.Message(), "leader=")
	return strings.TrimSpace(addr), true
}

type Client struct {
	seeds []string
	opts  []grpc.DialOption
	log   *slog.Logger

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	leader string
}

var _ grpcapi.CoordinatorServiceClient = (*Client)(nil)

// attemptTimeout bounds each try, so a call aimed at a leader that just died
// moves on quickly. (Connecting to a dead host's IP gets no answer at all;
// without a bound it waits for gRPC's 20 second connect timeout.)
const attemptTimeout = 3 * time.Second

// New returns a client for the coordinators at seeds (host:port, comma
// separated in config). Connections are made lazily.
func New(seeds []string, opts ...grpc.DialOption) (*Client, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("no coordinator addresses")
	}
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: 2 * time.Second,
		Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 2 * time.Second},
	}))
	return &Client{seeds: seeds, opts: opts, log: slog.Default(), conns: make(map[string]*grpc.ClientConn)}, nil
}

// Close closes every connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.conns = map[string]*grpc.ClientConn{}
}

// Leader returns the last address that answered as leader, or "".
func (c *Client) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, c.opts...)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// target picks where to send attempt n: the known leader, else a seed.
func (c *Client) target(n int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.seeds[n%len(c.seeds)]
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != addr {
		c.leader = addr
	}
}

// forget drops a dead or deposed leader so the next attempt uses the seeds.
// Its connection stays open: other calls may be using it, and gRPC
// reconnects by itself if the address comes back.
func (c *Client) forget(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader == addr {
		c.leader = ""
	}
}

// call runs fn against the leader, following redirects and retrying while
// there is no reachable leader, until ctx ends.
func call[T any](ctx context.Context, c *Client, fn func(context.Context, grpcapi.CoordinatorServiceClient) (T, error)) (T, error) {
	backoff := 50 * time.Millisecond
	for n := 0; ; n++ {
		addr := c.target(n)
		conn, err := c.conn(addr)
		if err != nil {
			var zero T
			return zero, err
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		resp, err := fn(attemptCtx, grpcapi.NewCoordinatorServiceClient(conn))
		cancel()
		if err == nil {
			c.setLeader(addr)
			return resp, nil
		}

		leader, notLeader := leaderFrom(err)
		switch {
		case notLeader && leader != "" && leader != addr && n < 20:
			// Redirect straight to the leader.
			c.setLeader(leader)
			continue
		case notLeader, status.Code(err) == codes.Unavailable,
			// A timeout or cancellation of this attempt, not of the caller.
			status.Code(err) == codes.DeadlineExceeded && ctx.Err() == nil,
			status.Code(err) == codes.Canceled && ctx.Err() == nil:
			// No leader yet, or this one is gone or not answering: wait and
			// retry. Every call is safe to repeat: task transitions are
			// guarded by attempt and status, and submissions carry
			// idempotency keys.
			c.forget(addr)
		default:
			return resp, err
		}

		select {
		case <-ctx.Done():
			return resp, err
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Second)
	}
}

func (c *Client) SubmitTask(ctx context.Context, in *grpcapi.ClientTaskRequest, opts ...grpc.CallOption) (*grpcapi.ClientTaskResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.ClientTaskResponse, error) {
		return cl.SubmitTask(ctx, in, opts...)
	})
}

func (c *Client) CancelTask(ctx context.Context, in *grpcapi.CancelTaskRequest, opts ...grpc.CallOption) (*grpcapi.CancelTaskResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.CancelTaskResponse, error) {
		return cl.CancelTask(ctx, in, opts...)
	})
}

func (c *Client) GetTaskOutput(ctx context.Context, in *grpcapi.TaskOutputRequest, opts ...grpc.CallOption) (*grpcapi.TaskOutputResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.TaskOutputResponse, error) {
		return cl.GetTaskOutput(ctx, in, opts...)
	})
}

func (c *Client) SendHeartbeat(ctx context.Context, in *grpcapi.HeartbeatRequest, opts ...grpc.CallOption) (*grpcapi.HeartbeatResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.HeartbeatResponse, error) {
		return cl.SendHeartbeat(ctx, in, opts...)
	})
}

func (c *Client) UpdateTaskStatus(ctx context.Context, in *grpcapi.UpdateTaskStatusRequest, opts ...grpc.CallOption) (*grpcapi.UpdateTaskStatusResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.UpdateTaskStatusResponse, error) {
		return cl.UpdateTaskStatus(ctx, in, opts...)
	})
}

func (c *Client) SubmitWorkflow(ctx context.Context, in *grpcapi.WorkflowRequest, opts ...grpc.CallOption) (*grpcapi.WorkflowResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.WorkflowResponse, error) {
		return cl.SubmitWorkflow(ctx, in, opts...)
	})
}

func (c *Client) CancelWorkflow(ctx context.Context, in *grpcapi.CancelWorkflowRequest, opts ...grpc.CallOption) (*grpcapi.CancelWorkflowResponse, error) {
	return call(ctx, c, func(ctx context.Context, cl grpcapi.CoordinatorServiceClient) (*grpcapi.CancelWorkflowResponse, error) {
		return cl.CancelWorkflow(ctx, in, opts...)
	})
}

// Seeds parses a comma-separated address list.
func Seeds(list string) []string {
	var out []string
	for _, s := range strings.Split(list, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
