package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	schedulerURL = flag.String("scheduler", "http://localhost:8081", "Scheduler URL")
	command      = flag.String("cmd", "", "Command to submit")
	action       = flag.String("action", "submit", "Action: submit, schedule, status, test, workflow, workflow-status, workflow-test")
	taskID       = flag.String("id", "", "Task/Workflow ID for status check")
	delay        = flag.Int("delay", 0, "Delay in seconds for scheduled tasks")
	wfType       = flag.String("type", "trip_booking", "Workflow type to submit")
	wfInput      = flag.String("input", "", "Workflow input JSON")
)


type TaskRequest struct {
	Data         string `json:"data"`
	DelaySeconds int    `json:"delay_seconds,omitempty"`
}

type TaskResponse struct {
	TaskID  string `json:"task_id"`
	Message string `json:"message"`
	Success bool   `json:"success"`
}

type TaskStatus struct {
	TaskID      string  `json:"task_id"`
	Data        string  `json:"data"`
	Status      string  `json:"status"`
	ScheduledAt string  `json:"scheduled_at"`
	StartedAt   *string `json:"started_at"`
	CompletedAt *string `json:"completed_at"`
	FailedAt    *string `json:"failed_at"`
}


func main() {
	flag.Parse()

	switch *action {
	case "submit":
		submitTask()
	case "schedule":
		scheduleTask()
	case "status":
		checkStatus()
	case "test":
		runFullTest()
	case "workflow":
		submitWorkflow()
	case "workflow-status":
		checkWorkflowStatus()
	case "workflow-test":
		runWorkflowTest()
	default:
		fmt.Println("Usage:")
		fmt.Println("  Submit task:       client -action=submit -cmd=\"echo hello\"")
		fmt.Println("  Schedule task:     client -action=schedule -cmd=\"echo hello\" -delay=30")
		fmt.Println("  Check status:      client -action=status -id=<task-id>")
		fmt.Println("  Run full test:     client -action=test")
		fmt.Println("  Submit workflow:   client -action=workflow -type=trip_booking -input='{\"user_id\":123,\"amount\":5000}'")
		fmt.Println("  Workflow status:   client -action=workflow-status -id=<workflow-id>")
		fmt.Println("  Workflow test:     client -action=workflow-test")
		os.Exit(1)
	}
}

func submitTask() {
	if *command == "" {
		log.Fatal("Command is required. Use -cmd=\"your command\"")
	}

	req := TaskRequest{Data: *command}
	resp, err := postJSON(*schedulerURL+"/tasks", req)
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	var taskResp TaskResponse
	if err := json.Unmarshal(resp, &taskResp); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	fmt.Printf("Task submitted!\n")
	fmt.Printf("Task ID: %s\n", taskResp.TaskID)
	fmt.Printf("Message: %s\n", taskResp.Message)
}

func scheduleTask() {
	if *command == "" {
		log.Fatal("Command is required. Use -cmd=\"your command\"")
	}
	if *delay <= 0 {
		log.Fatal("Delay is required. Use -delay=30")
	}

	req := TaskRequest{Data: *command, DelaySeconds: *delay}
	resp, err := postJSON(*schedulerURL+"/tasks/schedule", req)
	if err != nil {
		log.Fatalf("Failed to schedule task: %v", err)
	}

	var taskResp TaskResponse
	if err := json.Unmarshal(resp, &taskResp); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	fmt.Printf("Task scheduled!\n")
	fmt.Printf("Task ID: %s\n", taskResp.TaskID)
	fmt.Printf("Will run in: %d seconds\n", *delay)
}

func checkStatus() {
	if *taskID == "" {
		log.Fatal("Task ID is required. Use -id=<task-id>")
	}

	resp, err := http.Get(fmt.Sprintf("%s/tasks/status?id=%s", *schedulerURL, *taskID))
	if err != nil {
		log.Fatalf("Failed to check status: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var status TaskStatus
	if err := json.Unmarshal(body, &status); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	fmt.Printf("Task Status\n")
	fmt.Printf("   ID:          %s\n", status.TaskID)
	fmt.Printf("   Data:        %s\n", status.Data)
	fmt.Printf("   Status:      %s\n", formatStatus(status.Status))
	fmt.Printf("   Scheduled:   %s\n", status.ScheduledAt)
	if status.StartedAt != nil {
		fmt.Printf("   Started:     %s\n", *status.StartedAt)
	}
	if status.CompletedAt != nil {
		fmt.Printf("   Completed:   %s\n", *status.CompletedAt)
	}
	if status.FailedAt != nil {
		fmt.Printf("   Failed:      %s\n", *status.FailedAt)
	}
}

func runFullTest() {
	fmt.Println("Running Task Scheduler Test Suite")
	fmt.Println()

	// Test 1: Health check
	fmt.Println("1. Testing health endpoint...")
	resp, err := http.Get(*schedulerURL + "/health")
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("Health check returned status %d", resp.StatusCode)
	}
	fmt.Println("Scheduler is healthy")
	fmt.Println()

	// Test 2: Submit immediate task
	fmt.Println("2. Submitting immediate task...")
	taskReq := TaskRequest{Data: "echo 'Hello from task scheduler!'"}
	body, err := postJSON(*schedulerURL+"/tasks", taskReq)
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	var taskResp TaskResponse
	json.Unmarshal(body, &taskResp)
	if !taskResp.Success {
		log.Fatalf("Task submission failed: %s", taskResp.Message)
	}
	fmt.Printf("Task created: %s\n", taskResp.TaskID)
	fmt.Println()

	// Test 3: Check task status
	fmt.Println("3. Checking task status...")
	time.Sleep(2 * time.Second) 

	statusResp, err := http.Get(fmt.Sprintf("%s/tasks/status?id=%s", *schedulerURL, taskResp.TaskID))
	if err != nil {
		log.Fatalf("Failed to check status: %v", err)
	}
	defer statusResp.Body.Close()
	body, _ = io.ReadAll(statusResp.Body)

	var status TaskStatus
	json.Unmarshal(body, &status)
	fmt.Printf("Task status: %s\n", formatStatus(status.Status))
	fmt.Println()
	fmt.Println("4. Submitting scheduled task (5 second delay)...")
	schedReq := TaskRequest{Data: "echo 'Delayed task executed!'", DelaySeconds: 5}
	body, err = postJSON(*schedulerURL+"/tasks/schedule", schedReq)
	if err != nil {
		log.Fatalf("Failed to schedule task: %v", err)
	}

	json.Unmarshal(body, &taskResp)
	if !taskResp.Success {
		log.Fatalf("Task scheduling failed: %s", taskResp.Message)
	}
	fmt.Printf("Scheduled task created: %s\n", taskResp.TaskID)
	fmt.Println()
	fmt.Println("Monitoring scheduled task...")
	scheduledTaskID := taskResp.TaskID

	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		statusResp, _ := http.Get(fmt.Sprintf("%s/tasks/status?id=%s", *schedulerURL, scheduledTaskID))
		body, _ := io.ReadAll(statusResp.Body)
		statusResp.Body.Close()

		json.Unmarshal(body, &status)
		fmt.Printf("   [%ds] Status: %s\n", i+1, formatStatus(status.Status))

		if status.Status == "COMPLETE" || status.Status == "FAILED" {
			break
		}
	}
	fmt.Println()
	fmt.Println("Test suite completed!")
	fmt.Println()
	fmt.Println("If workers are running, tasks should transition:")
	fmt.Println("  QUEUED → STARTED → COMPLETE")
	fmt.Println()
	fmt.Println("If tasks stay QUEUED, make sure workers are running.")
}

// --- Workflow types ---

type WorkflowSubmitRequest struct {
	WorkflowType string          `json:"workflow_type"`
	Input        json.RawMessage `json:"input"`
}

type WorkflowSubmitResponse struct {
	WorkflowID string `json:"workflow_id"`
	Message    string `json:"message"`
	Success    bool   `json:"success"`
}

type WorkflowStatusResp struct {
	WorkflowID   string              `json:"workflow_id"`
	WorkflowType string              `json:"workflow_type"`
	Status       string              `json:"status"`
	CurrentStep  int                 `json:"current_step"`
	ErrorMessage string              `json:"error_message,omitempty"`
	Steps        []WorkflowStepResp  `json:"steps"`
	CreatedAt    string              `json:"created_at"`
	UpdatedAt    string              `json:"updated_at"`
}

type WorkflowStepResp struct {
	StepNumber         int    `json:"step_number"`
	Name               string `json:"name"`
	Status             string `json:"status"`
	TaskID             string `json:"task_id,omitempty"`
	CompensationTaskID string `json:"compensation_task_id,omitempty"`
}

func submitWorkflow() {
	inputJSON := json.RawMessage("{}")
	if *wfInput != "" {
		inputJSON = json.RawMessage(*wfInput)
	}

	req := WorkflowSubmitRequest{
		WorkflowType: *wfType,
		Input:        inputJSON,
	}
	resp, err := postJSON(*schedulerURL+"/workflows", req)
	if err != nil {
		log.Fatalf("Failed to submit workflow: %v", err)
	}

	var wfResp WorkflowSubmitResponse
	if err := json.Unmarshal(resp, &wfResp); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	if !wfResp.Success {
		log.Fatalf("Workflow submission failed: %s", wfResp.Message)
	}

	fmt.Printf("Workflow submitted!\n")
	fmt.Printf("  Workflow ID: %s\n", wfResp.WorkflowID)
	fmt.Printf("  Type:        %s\n", *wfType)
	fmt.Printf("  Message:     %s\n", wfResp.Message)
}

func checkWorkflowStatus() {
	if *taskID == "" {
		log.Fatal("Workflow ID is required. Use -id=<workflow-id>")
	}

	resp, err := http.Get(fmt.Sprintf("%s/workflows/status?id=%s", *schedulerURL, *taskID))
	if err != nil {
		log.Fatalf("Failed to check workflow status: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var status WorkflowStatusResp
	if err := json.Unmarshal(body, &status); err != nil {
		log.Fatalf("Failed to parse response: %v", err)
	}

	fmt.Printf("Workflow Status\n")
	fmt.Printf("  ID:          %s\n", status.WorkflowID)
	fmt.Printf("  Type:        %s\n", status.WorkflowType)
	fmt.Printf("  Status:      %s\n", status.Status)
	fmt.Printf("  Current Step: %d\n", status.CurrentStep)
	if status.ErrorMessage != "" {
		fmt.Printf("  Error:       %s\n", status.ErrorMessage)
	}
	fmt.Printf("  Created:     %s\n", status.CreatedAt)
	fmt.Println()
	fmt.Println("  Steps:")
	for _, step := range status.Steps {
		fmt.Printf("    [%d] %s - %s\n", step.StepNumber, step.Name, step.Status)
		if step.TaskID != "" {
			fmt.Printf("        Task: %s\n", step.TaskID)
		}
		if step.CompensationTaskID != "" {
			fmt.Printf("        Compensation: %s\n", step.CompensationTaskID)
		}
	}
}

func runWorkflowTest() {
	fmt.Println("Running Workflow (Saga) Test Suite")
	fmt.Println()

	// Test 1: Submit a trip_booking workflow
	fmt.Println("1. Submitting trip_booking workflow...")
	input := json.RawMessage(`{"user_id": 123, "amount": 5000}`)
	req := WorkflowSubmitRequest{
		WorkflowType: "trip_booking",
		Input:        input,
	}
	body, err := postJSON(*schedulerURL+"/workflows", req)
	if err != nil {
		log.Fatalf("Failed to submit workflow: %v", err)
	}

	var wfResp WorkflowSubmitResponse
	json.Unmarshal(body, &wfResp)
	if !wfResp.Success {
		log.Fatalf("Workflow submission failed: %s", wfResp.Message)
	}
	fmt.Printf("   Workflow created: %s\n", wfResp.WorkflowID)
	fmt.Println()

	// Test 2: Monitor workflow progress
	fmt.Println("2. Monitoring workflow progress...")
	wfID := wfResp.WorkflowID

	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)

		statusResp, err := http.Get(fmt.Sprintf("%s/workflows/status?id=%s", *schedulerURL, wfID))
		if err != nil {
			fmt.Printf("   [%ds] Error checking status: %v\n", i+1, err)
			continue
		}
		body, _ := io.ReadAll(statusResp.Body)
		statusResp.Body.Close()

		var status WorkflowStatusResp
		json.Unmarshal(body, &status)

		stepSummary := ""
		for _, s := range status.Steps {
			stepSummary += fmt.Sprintf(" [%s:%s]", s.Name, s.Status)
		}
		fmt.Printf("   [%ds] Workflow: %s |%s\n", i+1, status.Status, stepSummary)

		if status.Status == "COMPLETED" || status.Status == "FAILED" {
			break
		}
	}

	fmt.Println()
	fmt.Println("Workflow test completed!")
	fmt.Println()
	fmt.Println("Expected flow:")
	fmt.Println("  Book Flight -> Book Hotel -> Charge Payment -> COMPLETED")
	fmt.Println()
	fmt.Println("If a step fails, you should see compensation (rollback):")
	fmt.Println("  Step FAILED -> COMPENSATING -> Cancel previous steps -> FAILED")
}

func postJSON(url string, data interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func formatStatus(status string) string {
	switch status {
	case "QUEUED":
		return "QUEUED"
	case "STARTED":
		return "STARTED"
	case "COMPLETED":
		return "COMPLETED"
	case "FAILED":
		return "FAILED"
	default:
		return status
	}
}
