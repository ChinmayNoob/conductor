package main

import (
	"flag"
	"log"
	"net/http"
	"encoding/json"
	"time"


	"github.com/ChinmayNoob/conductor/pkg/db"
	"github.com/ChinmayNoob/conductor/pkg/scheduler"
)

var (
	schedulerPort   = flag.String("scheduler_port", ":8081", "The Scheduler HTTP Server Port")
	coordinatorAddr = flag.String("coordinator", "localhost:8080", "The Coordinator Address")
)

var sched *scheduler.Server
var database *db.DB

func main() {
	flag.Parse()
	log.Println("Starting Scheduler Service...")

	var err error
	database, err = db.New()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()

	log.Println("Connected to database")

	sched, err = scheduler.NewServer(database, *coordinatorAddr)

	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)

	}
	defer sched.Close()
	http.HandleFunc("/tasks", corsMiddleware(handleTasks))
	http.HandleFunc("/tasks/list", corsMiddleware(handleListTasks))
	http.HandleFunc("/tasks/schedule", corsMiddleware(handleScheduleTask))
	http.HandleFunc("/tasks/status", corsMiddleware(handleTaskStatus))
	http.HandleFunc("/tasks/stats", corsMiddleware(handleStats))
	http.HandleFunc("/health", corsMiddleware(handleHealth))

	log.Printf("Scheduler HTTP server listening on %s", *schedulerPort)
	log.Println("Endpoints:")
	log.Println("  POST /tasks          - Submit a task for immediate execution")
	log.Println("  GET  /tasks/list     - List all tasks")
	log.Println("  POST /tasks/schedule - Schedule a task for later")
	log.Println("  GET  /tasks/status   - Get task status")
	log.Println("  GET  /tasks/stats    - Get task statistics")
	log.Println("  GET  /health         - Health check")

	if err := http.ListenAndServe(*schedulerPort, nil); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

type TaskRequest struct {
	Data string `json:"data"`
	Priority int `json:"priority,omitempty"`
	MaxRetries int `json:"max_retries,omitempty"`
	RetryDelaySeconds int `json:"retry_delay_seconds,omitempty"`
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
	DelaySeconds int `json:"delay_seconds,omitempty"`
	ScheduledAt int64 `json:"scheduled_at,omitempty"`
}

type TaskResponse struct {
	TaskID string `json:"task_id"`
	Message string `json:"message"`
	Success bool `json:"success"`
}

type TaskStatusResponse struct {
	TaskID string `json:"task_id"`
	Data string `json:"data"`
	Status string `json:"status"`
	Priority int `json:"priority"`
	MaxRetries int `json:"max_retries"`
	RetryCount int `json:"retry_count"`
	TimeoutSeconds int `json:"timeout_seconds"`
	ScheduledAt    string  `json:"scheduled_at"`
	StartedAt      *string `json:"started_at,omitempty"`
	CompletedAt    *string `json:"completed_at,omitempty"`
	FailedAt       *string `json:"failed_at,omitempty"`
	Output         string  `json:"output,omitempty"`
	ErrorMessage   string  `json:"error_message,omitempty"`
}

type StatsResponse struct {
	TotalTasks int `json:"total_tasks"`
	Queued     int `json:"queued"`
	Started    int `json:"started"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

func handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, TaskResponse{Message: "Invalid JSON", Success: false}, http.StatusBadRequest)
		return
	}

	if req.Data == "" {
		sendJSON(w, TaskResponse{Message: "Data field is required", Success: false}, http.StatusBadRequest)
		return
	}

	// Build task options
	opts := db.DefaultTaskOptions()
	if req.Priority > 0 && req.Priority <= 10 {
		opts.Priority = req.Priority
	}
	if req.MaxRetries > 0 {
		opts.MaxRetries = req.MaxRetries
	}
	if req.RetryDelaySeconds > 0 {
		opts.RetryDelaySeconds = req.RetryDelaySeconds
	}
	if req.TimeoutSeconds > 0 {
		opts.TimeoutSeconds = req.TimeoutSeconds
	}

	taskID, err := sched.ScheduleTaskWithOptions(req.Data, opts)
	if err != nil {
		log.Printf("Failed to schedule task: %v", err)
		sendJSON(w, TaskResponse{Message: "Failed to schedule task", Success: false}, http.StatusInternalServerError)
		return
	}

	sendJSON(w, TaskResponse{
		TaskID:  taskID,
		Message: "Task queued successfully",
		Success: true,
	}, http.StatusCreated)
}

func handleListTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get status filter from query params
	status := r.URL.Query().Get("status")

	var tasks []*db.Task
	var err error

	if status != "" {
		tasks, err = database.ListTasksByStatus(db.TaskStatus(status), 100)
	} else {
		// Get all tasks (combine all statuses)
		var allTasks []*db.Task
		for _, s := range []db.TaskStatus{db.StatusQueued, db.StatusStarted, db.StatusCompleted, db.StatusFailed} {
			t, e := database.ListTasksByStatus(s, 50)
			if e != nil {
				err = e
				break
			}
			allTasks = append(allTasks, t...)
		}
		tasks = allTasks
	}

	if err != nil {
		log.Printf("Failed to list tasks: %v", err)
		sendJSON(w, TaskResponse{Message: "Failed to list tasks", Success: false}, http.StatusInternalServerError)
		return
	}

	// Convert to response format
	var response []TaskStatusResponse
	for _, task := range tasks {
		resp := TaskStatusResponse{
			TaskID:         task.ID.String(),
			Data:           task.Data,
			Status:         string(task.Status),
			Priority:       task.Priority,
			MaxRetries:     task.MaxRetries,
			RetryCount:     task.RetryCount,
			TimeoutSeconds: task.TimeoutSeconds,
			ScheduledAt:    task.ScheduledAt.Format(time.RFC3339),
			Output:         task.Output,
			ErrorMessage:   task.ErrorMessage,
		}

		if task.StartedAt != nil {
			s := task.StartedAt.Format(time.RFC3339)
			resp.StartedAt = &s
		}
		if task.CompletedAt != nil {
			s := task.CompletedAt.Format(time.RFC3339)
			resp.CompletedAt = &s
		}
		if task.FailedAt != nil {
			s := task.FailedAt.Format(time.RFC3339)
			resp.FailedAt = &s
		}

		response = append(response, resp)
	}

	sendJSON(w, response, http.StatusOK)
}

func handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, TaskResponse{Message: "Invalid JSON", Success: false}, http.StatusBadRequest)
		return
	}

	if req.Data == "" {
		sendJSON(w, TaskResponse{Message: "Data field is required", Success: false}, http.StatusBadRequest)
		return
	}

	// Build task options
	opts := db.DefaultTaskOptions()
	if req.Priority > 0 && req.Priority <= 10 {
		opts.Priority = req.Priority
	}
	if req.MaxRetries > 0 {
		opts.MaxRetries = req.MaxRetries
	}
	if req.RetryDelaySeconds > 0 {
		opts.RetryDelaySeconds = req.RetryDelaySeconds
	}
	if req.TimeoutSeconds > 0 {
		opts.TimeoutSeconds = req.TimeoutSeconds
	}

	// Handle scheduling time
	if req.ScheduledAt != 0 {
		opts.ScheduledAt = time.Unix(req.ScheduledAt, 0).UTC()
	} else if req.DelaySeconds > 0 {
		opts.ScheduledAt = time.Now().UTC().Add(time.Duration(req.DelaySeconds) * time.Second)
	} else {
		sendJSON(w, TaskResponse{Message: "Either delay_seconds or scheduled_at is required", Success: false}, http.StatusBadRequest)
		return
	}

	taskID, err := sched.ScheduleTaskWithOptions(req.Data, opts)
	if err != nil {
		log.Printf("Failed to schedule task: %v", err)
		sendJSON(w, TaskResponse{Message: "Failed to schedule task", Success: false}, http.StatusInternalServerError)
		return
	}

	sendJSON(w, TaskResponse{
		TaskID:  taskID,
		Message: "Task scheduled successfully",
		Success: true,
	}, http.StatusCreated)
}

func handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID := r.URL.Query().Get("id")
	if taskID == "" {
		sendJSON(w, TaskResponse{Message: "Task ID is required", Success: false}, http.StatusBadRequest)
		return
	}

	task, err := sched.GetTaskStatus(taskID)
	if err != nil {
		log.Printf("Failed to get task status: %v", err)
		sendJSON(w, TaskResponse{Message: "Failed to get task status", Success: false}, http.StatusInternalServerError)
		return
	}

	if task == nil {
		sendJSON(w, TaskResponse{Message: "Task not found", Success: false}, http.StatusNotFound)
		return
	}

	resp := TaskStatusResponse{
		TaskID:         task.ID.String(),
		Data:           task.Data,
		Status:         string(task.Status),
		Priority:       task.Priority,
		MaxRetries:     task.MaxRetries,
		RetryCount:     task.RetryCount,
		TimeoutSeconds: task.TimeoutSeconds,
		ScheduledAt:    task.ScheduledAt.Format(time.RFC3339),
		Output:         task.Output,
		ErrorMessage:   task.ErrorMessage,
	}

	if task.StartedAt != nil {
		s := task.StartedAt.Format(time.RFC3339)
		resp.StartedAt = &s
	}
	if task.CompletedAt != nil {
		s := task.CompletedAt.Format(time.RFC3339)
		resp.CompletedAt = &s
	}
	if task.FailedAt != nil {
		s := task.FailedAt.Format(time.RFC3339)
		resp.FailedAt = &s
	}

	sendJSON(w, resp, http.StatusOK)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := StatsResponse{}

	// Count tasks by status
	for _, s := range []db.TaskStatus{db.StatusQueued, db.StatusStarted, db.StatusCompleted, db.StatusFailed} {
		tasks, err := database.ListTasksByStatus(s, 1000)
		if err != nil {
			continue
		}
		count := len(tasks)
		stats.TotalTasks += count

		switch s {
		case db.StatusQueued:
			stats.Queued = count
		case db.StatusStarted:
			stats.Started = count
		case db.StatusCompleted:
			stats.Completed = count
		case db.StatusFailed:
			stats.Failed = count
		}
	}

	sendJSON(w, stats, http.StatusOK)
}





func handleHealth(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]string{"status": "healthy"}, http.StatusOK)
}

func sendJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}