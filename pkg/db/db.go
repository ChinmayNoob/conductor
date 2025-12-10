package db

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	uuid "github.com/jackc/pgx/pgtype/ext/gofrs-uuid"
	_ "github.com/lib/pq"
)

type TaskStatus string


const (
	StatusQueued TaskStatus = "QUEUED"
	StatusStarted TaskStatus = "STARTED"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed TaskStatus = "FAILED"
)

type Task struct {
	ID uuid.UUID
	Data string
	Status TaskStatus
	ScheduledAt time.Time
	PickedAt *time.Time
	StartedAt *time.Time
	CompletedAt *time.Time
	FailedAt *time.Time
	Priority int
	MaxRetries int
	RetryCount int
	RetryDelaySeconds int
	TimeoutSeconds int
	Output string
	ErrorMessage string
	CreatedAt time.Time
}

type TaskOptions struct {
	Priority int 
	MaxRetries int
	RetryDelaySeconds int
	TimeoutSeconds int
	ScheduledAt time.Time
}

func DefaultTaskOptions() TaskOptions {
	return TaskOptions{
		Priority: 5,
		MaxRetries: 3,
		RetryDelaySeconds: 60,
		TimeoutSeconds: 300,
		ScheduledAt: time.Now().UTC(),
	}
}





