package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kubernetes tasks (type k8s) run each attempt as its own pod, with its own
// image, resources and service account. Like the container executor, this
// is a small client for the few API calls needed (create, watch, read logs,
// delete a pod), not client-go and its dependency tree.

const (
	// Where the worker finds its service account credentials in a pod.
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	// Tasks write key=value outputs here ($CONDUCTOR_OUTPUT): the
	// container's termination message, which the kubelet keeps (up to 4 KB)
	// after the container exits.
	k8sOutputPath = "/dev/termination-log"
	// The kubelet keeps at most this much of a termination message.
	k8sMaxOutputs = 4096
	// The task's container in its pod.
	k8sContainer = "task"
	// The user task pods run as unless the task says otherwise: nobody.
	k8sDefaultUser = 65534
	// Labels on task pods.
	k8sLabelManagedBy = "app.kubernetes.io/managed-by"
	k8sLabelTask      = "conductor.dev/task-id"
	k8sLabelAttempt   = "conductor.dev/attempt"
	k8sLabelWorker    = "conductor.dev/worker"
	k8sLabelInstance  = "conductor.dev/instance"
	// Finished task pods older than this are deleted by any worker's sweep.
	// Workers delete their own pods as soon as they read the result, so
	// these are left over from workers that died.
	k8sOrphanAge = 10 * time.Minute
)

var labelValueRe = regexp.MustCompile(`^([A-Za-z0-9][-A-Za-z0-9_.]{0,61}[A-Za-z0-9]|[A-Za-z0-9]?)$`)

type k8sSpec struct {
	Image          string            `json:"image"`
	Command        []string          `json:"command"`
	Args           []string          `json:"args"`
	CPU            string            `json:"cpu"`
	Memory         string            `json:"memory"`
	ServiceAccount string            `json:"service_account"`
	NodeSelector   map[string]string `json:"node_selector"`
	RunAsUser      *int64            `json:"run_as_user"`
}

// KubeOptions enables Kubernetes tasks on a worker that runs in a cluster.
type KubeOptions struct {
	// Namespace task pods run in; default the worker's own.
	Namespace string
	// ServiceAccount runs tasks that don't name one. Its token is not
	// mounted. Default "default".
	ServiceAccount string
	// ServiceAccounts lists the other service accounts tasks may name (their
	// tokens are mounted). Anyone who can submit tasks gets what these
	// accounts may do, so keep the list short.
	ServiceAccounts []string
	// PodName and PodUID identify the worker's own pod. Task pods are
	// labelled with its name, and owned by it when they share its namespace,
	// so Kubernetes deletes them if the worker's pod goes away.
	PodName string
	PodUID  string
	// Instance names this Conductor installation (the Helm release), so
	// workers clean up only their own installation's task pods when
	// several share a namespace. Default "conductor".
	Instance string
}

// kube runs task pods through the Kubernetes API.
type kube struct {
	api       string // API server base URL
	http      *http.Client
	token     func() (string, error)
	namespace string
	opts      KubeOptions
	owner     []map[string]any // ownerReferences for task pods
	log       *slog.Logger
}

// inClusterKube connects with the pod's service account.
func inClusterKube(opts KubeOptions) (*kube, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in a Kubernetes pod (KUBERNETES_SERVICE_HOST is not set)")
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("no service account credentials: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid service account CA certificate")
	}
	ns, err := os.ReadFile(serviceAccountDir + "/namespace")
	if err != nil {
		return nil, fmt.Errorf("no service account namespace: %w", err)
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		MaxIdleConnsPerHost: 16,
	}}
	return newKube("https://"+net.JoinHostPort(host, port), client, fileToken(serviceAccountDir+"/token"),
		strings.TrimSpace(string(ns)), opts), nil
}

// fileToken reads a service account token, re-reading it every minute:
// the kubelet rotates projected tokens.
func fileToken(path string) func() (string, error) {
	var (
		mu     sync.Mutex
		token  string
		readAt time.Time
	)
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(readAt) > time.Minute {
			b, err := os.ReadFile(path)
			if err != nil && token == "" {
				return "", fmt.Errorf("failed to read service account token: %w", err)
			}
			if err == nil {
				token, readAt = strings.TrimSpace(string(b)), time.Now()
			}
		}
		return token, nil
	}
}

func newKube(api string, client *http.Client, token func() (string, error), workerNamespace string, opts KubeOptions) *kube {
	if opts.Namespace == "" {
		opts.Namespace = workerNamespace
	}
	if opts.ServiceAccount == "" {
		opts.ServiceAccount = "default"
	}
	if opts.Instance == "" {
		opts.Instance = "conductor"
	}
	k := &kube{api: api, http: client, token: token, namespace: opts.Namespace, opts: opts, log: slog.Default()}
	// Owner references can't cross namespaces.
	if opts.PodName != "" && opts.PodUID != "" && opts.Namespace == workerNamespace {
		k.owner = []map[string]any{{
			"apiVersion": "v1", "kind": "Pod", "name": opts.PodName, "uid": opts.PodUID,
		}}
	}
	return k
}

// kubeError is an error response from the API server.
type kubeError struct {
	Code    int
	Reason  string
	Message string
}

func (e *kubeError) Error() string {
	return fmt.Sprintf("kubernetes: %s (HTTP %d)", e.Message, e.Code)
}

func isKubeCode(err error, codes ...int) bool {
	var ke *kubeError
	return errors.As(err, &ke) && slices.Contains(codes, ke.Code)
}

// request sends a request and returns the response, or a *kubeError for a
// status of 300 or above.
func (k *kube) request(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, k.api+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	token, err := k.token()
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := k.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		var st struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		if json.Unmarshal(msg, &st) != nil || st.Message == "" {
			st.Message = strings.TrimSpace(string(msg))
		}
		return nil, &kubeError{Code: resp.StatusCode, Reason: st.Reason, Message: st.Message}
	}
	return resp, nil
}

func (k *kube) do(ctx context.Context, method, path string, body, out any) error {
	resp, err := k.request(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

func (k *kube) podsPath() string {
	return "/api/v1/namespaces/" + url.PathEscape(k.namespace) + "/pods"
}

// pod is the part of a Pod object the executor reads.
type pod struct {
	Metadata struct {
		Name              string            `json:"name"`
		ResourceVersion   string            `json:"resourceVersion"`
		Labels            map[string]string `json:"labels"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase             string            `json:"phase"`
		Reason            string            `json:"reason"`
		Message           string            `json:"message"`
		Conditions        []podCondition    `json:"conditions"`
		ContainerStatuses []containerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type podCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type containerStatus struct {
	Name  string `json:"name"`
	State struct {
		Waiting *struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"waiting"`
		Running    *struct{} `json:"running"`
		Terminated *struct {
			ExitCode   int       `json:"exitCode"`
			Reason     string    `json:"reason"`
			Message    string    `json:"message"`
			FinishedAt time.Time `json:"finishedAt"`
		} `json:"terminated"`
	} `json:"state"`
}

func (p *pod) container() *containerStatus {
	for i := range p.Status.ContainerStatuses {
		if p.Status.ContainerStatuses[i].Name == k8sContainer {
			return &p.Status.ContainerStatuses[i]
		}
	}
	return nil
}

func (p *pod) finished() bool {
	return p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed"
}

// Container waiting reasons that mean the pod will never run as specified.
var (
	permanentWaits = []string{"InvalidImageName", "ErrImageNeverPull"}
	// The kubelet keeps retrying these, but a retry of the task (which may
	// land on another node, after a fix) is the better place to wait.
	failedWaits = []string{"ImagePullBackOff", "CreateContainerConfigError", "CreateContainerError"}
)

var errPodGone = errors.New("the task's pod was deleted")

// run runs one attempt of a task as a pod and returns its output, the
// outputs it wrote to $CONDUCTOR_OUTPUT, and an error unless the container
// exited 0. The pod is deleted afterwards, also on cancellation.
func (k *kube) run(ctx context.Context, taskID string, attempt int32, specJSON []byte, env map[string]string,
	timeout time.Duration, out *cappedBuffer) (string, map[string]string, error) {
	var spec k8sSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return "", nil, permanent(fmt.Errorf("invalid k8s spec: %w", err))
	}
	manifest, err := k.podManifest(taskID, attempt, &spec, env, timeout)
	if err != nil {
		return "", nil, permanent(err)
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var created pod
	if err := k.do(runCtx, http.MethodPost, k.podsPath(), manifest, &created); err != nil {
		if isKubeCode(err, http.StatusBadRequest, http.StatusUnprocessableEntity) {
			err = permanent(err) // the pod spec is invalid
		} else {
			// The request may have reached the API server even though its
			// answer didn't reach us (a network error, or a cancel mid-call).
			k.deleteAttemptPods(taskID, attempt)
		}
		return "", nil, fmt.Errorf("failed to create the task's pod: %w", err)
	}
	name := created.Metadata.Name
	log := k.log.With("task_id", taskID, "pod", k.namespace+"/"+name)
	log.Info("Task pod created")
	defer k.deletePod(name, log)

	// Stream the container's logs into out from the moment it starts, so
	// they can be followed live.
	logCtx, stopLogs := context.WithCancel(context.WithoutCancel(ctx))
	defer stopLogs()
	var (
		following atomic.Bool
		logsDone  = make(chan error, 1)
	)
	startLogs := func() {
		if following.CompareAndSwap(false, true) {
			go func() { logsDone <- k.copyLogs(logCtx, name, true, out) }()
		}
	}

	final, waitErr := k.waitPod(runCtx, name, startLogs)
	if waitErr != nil {
		stopLogs()
		switch {
		case errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil:
			waitErr = fmt.Errorf("task timed out after %v%s", timeout, k.pendingReason(name))
		case ctx.Err() != nil:
			waitErr = context.Cause(ctx)
		}
		return out.String(), nil, waitErr
	}

	var reread *cappedBuffer
	if following.Load() {
		var followErr error
		select {
		case followErr = <-logsDone:
		case <-time.After(5 * time.Second):
			followErr = errors.New("log stream did not end")
		}
		stopLogs()
		if followErr != nil {
			// Read them again in one go.
			log.Debug("Following logs failed, reading them again", "error", followErr)
			full := newCappedBuffer(out.limit)
			if err := k.copyLogs(context.WithoutCancel(ctx), name, false, full); err == nil {
				reread = full
			}
		}
	}
	output := out.String()
	if reread != nil {
		output = reread.String()
	}
	outputs, err := podResult(final)
	return output, outputs, err
}

// podManifest builds the pod for a task attempt. Task pods get no
// privileges by default: a non-root user, no capabilities, no service
// account token, and nothing from Service environment variables.
func (k *kube) podManifest(taskID string, attempt int32, spec *k8sSpec, env map[string]string, timeout time.Duration) (map[string]any, error) {
	if spec.Image == "" {
		return nil, errors.New("k8s tasks need an image")
	}
	account, mountToken := k.opts.ServiceAccount, false
	if spec.ServiceAccount != "" && spec.ServiceAccount != k.opts.ServiceAccount {
		if !slices.Contains(k.opts.ServiceAccounts, spec.ServiceAccount) {
			return nil, fmt.Errorf("service account %q is not allowed for tasks (allowed: %s)",
				spec.ServiceAccount, strings.Join(append([]string{k.opts.ServiceAccount}, k.opts.ServiceAccounts...), ", "))
		}
		account, mountToken = spec.ServiceAccount, true
	}

	envList := []map[string]string{}
	keys := slices.Sorted(maps.Keys(env))
	for _, key := range keys {
		envList = append(envList, map[string]string{"name": key, "value": verbatim(env[key])})
	}
	envList = append(envList, map[string]string{"name": "CONDUCTOR_OUTPUT", "value": k8sOutputPath})

	resources := map[string]string{}
	if spec.CPU != "" {
		resources["cpu"] = spec.CPU
	}
	if spec.Memory != "" {
		resources["memory"] = spec.Memory
	}
	container := map[string]any{
		"name":                     k8sContainer,
		"image":                    spec.Image,
		"env":                      envList,
		"terminationMessagePath":   k8sOutputPath,
		"terminationMessagePolicy": "File",
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": false,
			"capabilities":             map[string]any{"drop": []string{"ALL"}},
		},
	}
	if len(spec.Command) > 0 {
		container["command"] = verbatimAll(spec.Command)
	}
	if len(spec.Args) > 0 {
		container["args"] = verbatimAll(spec.Args)
	}
	if len(resources) > 0 {
		// Requests equal limits: the task gets what it asked for, no more.
		container["resources"] = map[string]any{"requests": resources, "limits": resources}
	}

	user := int64(k8sDefaultUser)
	if spec.RunAsUser != nil {
		user = *spec.RunAsUser
	}
	podSecurity := map[string]any{
		"runAsUser":      user,
		"runAsGroup":     user,
		"seccompProfile": map[string]string{"type": "RuntimeDefault"},
	}
	if user != 0 {
		podSecurity["runAsNonRoot"] = true
	}

	labels := map[string]string{
		k8sLabelManagedBy: "conductor",
		k8sLabelInstance:  labelValue(k.opts.Instance),
		k8sLabelTask:      taskID,
		k8sLabelAttempt:   strconv.Itoa(int(attempt)),
	}
	if k.opts.PodName != "" {
		labels[k8sLabelWorker] = labelValue(k.opts.PodName)
	}
	metadata := map[string]any{"generateName": "conductor-task-", "labels": labels}
	if k.owner != nil {
		metadata["ownerReferences"] = k.owner
	}

	podSpec := map[string]any{
		"restartPolicy":                 "Never",
		"serviceAccountName":            account,
		"automountServiceAccountToken":  mountToken,
		"enableServiceLinks":            false,
		"terminationGracePeriodSeconds": 10,
		// A backstop in case the worker dies: the kubelet stops the pod a
		// minute after the task's timeout.
		"activeDeadlineSeconds": int64(timeout.Seconds()) + 60,
		"securityContext":       podSecurity,
		"containers":            []any{container},
	}
	if len(spec.NodeSelector) > 0 {
		podSpec["nodeSelector"] = spec.NodeSelector
	}
	return map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": podSpec}, nil
}

// verbatim escapes s for a pod's command, args or env value, where
// Kubernetes expands $(VAR) to another variable's value and turns $$ into $.
// Without it, a workflow input of "$(API_TOKEN)" would read the variable.
func verbatim(s string) string { return strings.ReplaceAll(s, "$", "$$") }

func verbatimAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = verbatim(s)
	}
	return out
}

// labelValue returns s if it is a valid label value, else a hash of it.
func labelValue(s string) string {
	if labelValueRe.MatchString(s) {
		return s
	}
	h := fnv.New64a()
	h.Write([]byte(s))
	return fmt.Sprintf("h%x", h.Sum64())
}

// waitPod waits for the pod to finish, calling started once its container
// is running (or has run). It returns early with an error if the container
// can't start.
func (k *kube) waitPod(ctx context.Context, name string, started func()) (*pod, error) {
	check := func(p *pod) (done bool, err error) {
		if c := p.container(); c != nil {
			if c.State.Running != nil || c.State.Terminated != nil {
				started()
			}
			if w := c.State.Waiting; w != nil {
				if slices.Contains(permanentWaits, w.Reason) {
					return true, permanent(fmt.Errorf("the task's container can't start: %s: %s", w.Reason, w.Message))
				}
				if slices.Contains(failedWaits, w.Reason) {
					return true, fmt.Errorf("the task's container can't start: %s: %s", w.Reason, w.Message)
				}
			}
		}
		return p.finished(), nil
	}

	for {
		var p pod
		err := k.do(ctx, http.MethodGet, k.podsPath()+"/"+url.PathEscape(name), nil, &p)
		if isKubeCode(err, http.StatusNotFound) {
			return nil, errPodGone
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			k.log.Warn("Failed to read task pod, retrying", "pod", name, "error", err)
			if !sleepCtx(ctx, time.Second) {
				return nil, ctx.Err()
			}
			continue
		}
		if done, err := check(&p); done {
			return &p, err
		}

		// Wait for changes from here. A watch ends after a while (or on a
		// network hiccup); then read the pod again and resume.
		final, err := k.watchPod(ctx, name, p.Metadata.ResourceVersion, check)
		if final != nil || err != nil {
			if errors.Is(err, errWatchEnded) {
				continue
			}
			return final, err
		}
	}
}

var errWatchEnded = errors.New("watch ended")

// watchPod follows changes to the pod from resourceVersion, until check
// says it's done. It returns errWatchEnded when the watch closes first.
func (k *kube) watchPod(ctx context.Context, name, resourceVersion string, check func(*pod) (bool, error)) (*pod, error) {
	q := url.Values{
		"watch":           {"true"},
		"fieldSelector":   {"metadata.name=" + name},
		"resourceVersion": {resourceVersion},
		"timeoutSeconds":  {"60"},
	}
	resp, err := k.request(ctx, http.MethodGet, k.podsPath()+"?"+q.Encode(), nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		k.log.Warn("Failed to watch task pod, retrying", "pod", name, "error", err)
		if !sleepCtx(ctx, time.Second) {
			return nil, ctx.Err()
		}
		return nil, errWatchEnded
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	for {
		var event struct {
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		}
		if err := dec.Decode(&event); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errWatchEnded
		}
		switch event.Type {
		case "ADDED", "MODIFIED":
			var p pod
			if err := json.Unmarshal(event.Object, &p); err != nil {
				return nil, errWatchEnded
			}
			if done, err := check(&p); done {
				return &p, err
			}
		case "DELETED":
			return nil, errPodGone
		default: // ERROR, e.g. the resource version is too old
			return nil, errWatchEnded
		}
	}
}

// pendingReason explains why a pod never started, for timeout errors: an
// unschedulable pod or an image still being pulled says so.
func (k *kube) pendingReason(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var p pod
	if k.do(ctx, http.MethodGet, k.podsPath()+"/"+url.PathEscape(name), nil, &p) != nil || p.Status.Phase != "Pending" {
		return ""
	}
	for _, c := range p.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			return fmt.Sprintf(" (the pod was never scheduled: %s)", c.Message)
		}
	}
	if c := p.container(); c != nil && c.State.Waiting != nil {
		return fmt.Sprintf(" (the container never started: %s)", c.State.Waiting.Reason)
	}
	return ""
}

// copyLogs copies the task container's logs into w; with follow, until the
// container exits.
func (k *kube) copyLogs(ctx context.Context, name string, follow bool, w io.Writer) error {
	q := url.Values{"container": {k8sContainer}}
	if follow {
		q.Set("follow", "true")
	}
	resp, err := k.request(ctx, http.MethodGet, k.podsPath()+"/"+url.PathEscape(name)+"/log?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	return err
}

// deleteAttemptPods deletes any pod of this task attempt.
func (k *kube) deleteAttemptPods(taskID string, attempt int32) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pods, err := k.listTaskPods(ctx, k8sLabelTask+"="+taskID+","+k8sLabelAttempt+"="+strconv.Itoa(int(attempt)))
	if err != nil {
		k.log.Warn("Failed to look for the pod of a failed create", "task_id", taskID, "error", err)
		return
	}
	for _, p := range pods {
		k.deletePod(p.Metadata.Name, k.log)
	}
}

// deletePod deletes a task pod, whatever state it's in.
func (k *kube) deletePod(name string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := k.do(ctx, http.MethodDelete, k.podsPath()+"/"+url.PathEscape(name),
		map[string]any{"gracePeriodSeconds": 1, "propagationPolicy": "Background"}, nil)
	if err != nil && !isKubeCode(err, http.StatusNotFound) {
		log.Warn("Failed to delete task pod", "error", err)
	}
}

// podResult reads a finished pod: the outputs its container wrote, and an
// error unless it exited 0.
func podResult(p *pod) (map[string]string, error) {
	c := p.container()
	if c == nil || c.State.Terminated == nil {
		// The pod failed around its container: evicted, or past its deadline.
		reason := p.Status.Reason
		if reason == "" {
			reason = "the pod failed"
		}
		return nil, fmt.Errorf("%s: %s", reason, p.Status.Message)
	}
	t := c.State.Terminated
	switch {
	case t.Reason == "OOMKilled":
		return nil, fmt.Errorf("the container ran out of memory (OOMKilled, exit status %d)", t.ExitCode)
	case t.ExitCode != 0:
		return nil, fmt.Errorf("container exited with status %d", t.ExitCode)
	case p.Status.Phase == "Failed":
		return nil, fmt.Errorf("the pod failed: %s %s", p.Status.Reason, p.Status.Message)
	case len(t.Message) >= k8sMaxOutputs:
		// The kubelet cut it: the last output would be truncated.
		return nil, permanent(fmt.Errorf("$CONDUCTOR_OUTPUT exceeds %d bytes, the most a k8s task can return", k8sMaxOutputs-1))
	}
	return parseOutputs([]byte(t.Message)), nil
}

// listTaskPods lists task pods matching a label selector (besides being
// Conductor's).
func (k *kube) listTaskPods(ctx context.Context, selector string) ([]pod, error) {
	sel := k8sLabelManagedBy + "=conductor," + k8sLabelInstance + "=" + labelValue(k.opts.Instance)
	if selector != "" {
		sel += "," + selector
	}
	var list struct {
		Items []pod `json:"items"`
	}
	err := k.do(ctx, http.MethodGet, k.podsPath()+"?"+url.Values{"labelSelector": {sel}}.Encode(), nil, &list)
	return list.Items, err
}

// deleteOwnLeftovers deletes the task pods this worker's pod started before
// the worker restarted in it. Call it before taking tasks: every pod
// labelled with this worker's pod is then a leftover.
func (k *kube) deleteOwnLeftovers(ctx context.Context) {
	if k.opts.PodName == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pods, err := k.listTaskPods(ctx, k8sLabelWorker+"="+labelValue(k.opts.PodName))
	if err != nil {
		k.log.Warn("Failed to list this worker's leftover task pods", "error", err)
		return
	}
	var wg sync.WaitGroup
	for _, p := range pods {
		k.log.Info("Deleting a task pod left from before a restart", "pod", p.Metadata.Name, "task_id", p.Metadata.Labels[k8sLabelTask])
		wg.Add(1)
		go func() {
			defer wg.Done()
			k.deletePod(p.Metadata.Name, k.log)
		}()
	}
	wg.Wait()
}

// sweepOrphans periodically deletes finished task pods nobody collected:
// workers delete their pods as soon as they read the result, so these were
// left by workers that died. (Their pods are also owned by the worker's pod
// when they share its namespace, so Kubernetes usually beats us to it.)
func (k *kube) sweepOrphans(ctx context.Context, every time.Duration) {
	for sleepCtx(ctx, every) {
		listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		pods, err := k.listTaskPods(listCtx, "")
		cancel()
		if err != nil {
			k.log.Warn("Failed to list task pods", "error", err)
			continue
		}
		for _, p := range pods {
			if p.orphaned(time.Now()) {
				k.log.Info("Deleting a leftover task pod", "pod", p.Metadata.Name, "task_id", p.Metadata.Labels[k8sLabelTask])
				k.deletePod(p.Metadata.Name, k.log)
			}
		}
	}
}

// orphaned reports whether a finished pod has sat uncollected for longer
// than k8sOrphanAge.
func (p *pod) orphaned(now time.Time) bool {
	if !p.finished() {
		return false
	}
	ended := p.Metadata.CreationTimestamp // failed before its container ran
	if c := p.container(); c != nil && c.State.Terminated != nil {
		ended = c.State.Terminated.FinishedAt
	}
	return now.Sub(ended) > k8sOrphanAge
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
