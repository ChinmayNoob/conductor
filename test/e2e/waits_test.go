//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ChinmayNoob/conductor/pkg/client"
)

// waitForStep polls a run until its step is waiting, and returns the run.
func waitForStep(t *testing.T, c *client.Client, id, step string) *client.Workflow {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		wf, err := c.GetWorkflow(ctxTimeout(t, 10*time.Second), id)
		if err != nil {
			t.Fatal(err)
		}
		if s := wf.Step(step); s != nil && s.Wait != nil && s.Wait.Waiting {
			return wf
		}
		if time.Now().After(deadline) {
			t.Fatalf("step %s never started waiting: %+v", step, wf.Step(step))
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func refundDefinition(name, approval string) string {
	return fmt.Sprintf(`
name: %s
inputs:
  amount: {required: true}
steps:
  - name: hold
    run: echo "holding $INPUT_AMOUNT"
    compensate: echo "releasing the hold"
  - name: approve
    depends_on: [hold]
    type: approval
    approval: %s
  - name: refund
    depends_on: [approve]
    run: 'echo "refunding $INPUT_AMOUNT, note: $NOTE"'
    env:
      NOTE: "${{ steps.approve.outputs.comment }}"
`, name, approval)
}

func TestApprovalApproved(t *testing.T) {
	c := newClient(t)
	name := uniqueName("refund")
	applyDefinition(t, c, refundDefinition(name, `{message: "Refund ${{ inputs.amount }} to the customer?"}`))
	wf := startWorkflow(t, c, name, map[string]any{"amount": "$40"})
	wf = waitForStep(t, c, wf.ID, "approve")
	if msg := wf.Step("approve").Wait.Message; msg != "Refund $40 to the customer?" {
		t.Fatalf("approval message %q", msg)
	}

	approvals, err := c.ListApprovals(ctxTimeout(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range approvals {
		found = found || (a.WorkflowID == wf.ID && a.Step == "approve")
	}
	if !found {
		t.Fatalf("the waiting approval is not listed: %+v", approvals)
	}

	if _, err := c.ApproveStep(ctxTimeout(t, 10*time.Second), wf.ID, "approve", "customer verified"); err != nil {
		t.Fatal(err)
	}
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	if wf.Status != "COMPLETED" {
		t.Fatalf("workflow %s: %s", wf.Status, wf.ErrorMessage)
	}
	refund := waitTask(t, c, wf.Step("refund").TaskID, time.Minute)
	if !strings.Contains(refund.Output, "refunding $40, note: customer verified") {
		t.Fatalf("refund output %q", refund.Output)
	}
	// Deciding twice is refused.
	if _, err := c.RejectStep(ctxTimeout(t, 10*time.Second), wf.ID, "approve", "too late"); client.StatusCode(err) != 409 {
		t.Fatalf("second decision: %v, want 409", err)
	}
}

func TestApprovalRejectedCompensates(t *testing.T) {
	c := newClient(t)
	name := uniqueName("refund")
	applyDefinition(t, c, refundDefinition(name, `{}`))
	wf := waitForStep(t, c, startWorkflow(t, c, name, map[string]any{"amount": "$900"}).ID, "approve")
	if _, err := c.RejectStep(ctxTimeout(t, 10*time.Second), wf.ID, "approve", "over the limit"); err != nil {
		t.Fatal(err)
	}
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	if wf.Status != "FAILED" || !strings.Contains(wf.ErrorMessage, "rejected") || !strings.Contains(wf.ErrorMessage, "over the limit") {
		t.Fatalf("workflow %s: %q", wf.Status, wf.ErrorMessage)
	}
	wantSteps(t, wf, map[string]string{"hold": "COMPENSATED", "approve": "FAILED", "refund": "SKIPPED"})
}

func TestApprovalTimeout(t *testing.T) {
	c := newClient(t)
	name := uniqueName("refund")
	applyDefinition(t, c, refundDefinition(name, `{timeout: 2s, on_timeout: approve}`))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, map[string]any{"amount": "$5"}).ID, time.Minute)
	if wf.Status != "COMPLETED" || wf.Step("approve").Wait.DecidedBy != "timeout" {
		t.Fatalf("workflow %s, approve %+v; want it approved by the timeout", wf.Status, wf.Step("approve").Wait)
	}
}

func signalDefinition(name, before, timeout string) string {
	return fmt.Sprintf(`
name: %s
steps:
  - name: invoice
    run: %q
  - name: paid
    depends_on: [invoice]
    type: signal
    signal: {name: payment%s}
  - name: receipt
    depends_on: [paid]
    run: echo "received $AMOUNT from $PAYER"
    env:
      AMOUNT: "${{ steps.paid.outputs.amount }}"
      PAYER: "${{ steps.paid.outputs.payer }}"
`, name, before, timeout)
}

func TestSignalDeliveredToWaitingStep(t *testing.T) {
	c := newClient(t)
	name := uniqueName("invoice")
	applyDefinition(t, c, signalDefinition(name, "echo sent", ""))
	wf := waitForStep(t, c, startWorkflow(t, c, name, nil).ID, "paid")
	delivered, err := c.Signal(ctxTimeout(t, 10*time.Second), wf.ID, "payment", map[string]any{"amount": 40, "payer": "ada"})
	if err != nil || !delivered {
		t.Fatalf("signal: delivered=%v err=%v", delivered, err)
	}
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	receipt := waitTask(t, c, wf.Step("receipt").TaskID, time.Minute)
	if wf.Status != "COMPLETED" || !strings.Contains(receipt.Output, "received 40 from ada") {
		t.Fatalf("workflow %s, receipt %q", wf.Status, receipt.Output)
	}
}

func TestSignalSentEarlyIsKept(t *testing.T) {
	c := newClient(t)
	name := uniqueName("invoice")
	applyDefinition(t, c, signalDefinition(name, "sleep 3; echo sent", ""))
	wf := startWorkflow(t, c, name, nil)
	// The payment arrives while the invoice step still runs.
	delivered, err := c.Signal(ctxTimeout(t, 10*time.Second), wf.ID, "payment", map[string]any{"amount": 12, "payer": "lin"})
	if err != nil || delivered {
		t.Fatalf("early signal: delivered=%v err=%v; want it kept", delivered, err)
	}
	wf = waitWorkflow(t, c, wf.ID, time.Minute)
	receipt := waitTask(t, c, wf.Step("receipt").TaskID, time.Minute)
	if wf.Status != "COMPLETED" || !strings.Contains(receipt.Output, "received 12 from lin") {
		t.Fatalf("workflow %s, receipt %q", wf.Status, receipt.Output)
	}
}

func TestSignalTimeoutFails(t *testing.T) {
	c := newClient(t)
	name := uniqueName("invoice")
	applyDefinition(t, c, signalDefinition(name, "echo sent", ", timeout: 2s"))
	wf := waitWorkflow(t, c, startWorkflow(t, c, name, nil).ID, time.Minute)
	if wf.Status != "FAILED" || !strings.Contains(wf.ErrorMessage, `no "payment" signal arrived in time`) {
		t.Fatalf("workflow %s: %q", wf.Status, wf.ErrorMessage)
	}
}
