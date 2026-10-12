# ADR 0004: Running on Kubernetes

- **Status:** Accepted (Phase 6)

## Context

Phase 6 makes Kubernetes a first-class home for Conductor: a Helm chart for
a highly available cluster, workers that scale on queue depth (to zero when
idle), tasks that run as their own pods, and an operator so workflows can
be managed with GitOps. Docker Compose stays the supported single-host
setup. The design principles still hold, above all *Postgres is the only
required dependency*: Kubernetes is where Conductor runs, not something it
needs.

## Decisions

**1. Each k8s task attempt is a bare Pod, created through a small REST
client.** Workers with `CONDUCTOR_K8S_TASKS` create one pod per attempt with
the API calls they need (create, watch, logs, delete), like the container
executor talks to Docker without its SDK. client-go would have tripled the
binary's dependency tree for five endpoints. A Job was the alternative: it
adds a controller that may retry pods itself, while Conductor already owns
retries, back-off and attempt fencing, so one attempt maps to exactly one
pod. What a Job would give, cleanup, comes from three cheaper mechanisms:
pods are owned by the worker's pod (garbage collected with it),
`activeDeadlineSeconds` stops a pod a minute past its task's timeout, and
workers delete pods left over from before a restart and finished pods
nobody collected.

**2. Least privilege is the default for task pods.** They run as user
65534 with no capabilities, no privilege escalation, RuntimeDefault seccomp,
no service-link environment variables and no service account token. A task
may name a service account only from the workers' allow-list; anyone who can
submit tasks gets what that account can do, so naming any other fails the
task permanently.

The workers that create those pods need a token, and a shell task runs as
its worker's user, so it could read that token and create any pod it liked.
So k8s tasks get a pool of their own (`CONDUCTOR_WORKER_TASK_TYPES=k8s`):
workers that run no task code, under the only service account with
permissions (a Role for pods and their logs in the task namespace, nothing
cluster-wide). Every other pool mounts no token at all, and the chart
refuses a pool that would run k8s tasks next to shell or container ones.

**3. `$CONDUCTOR_OUTPUT` is the termination message.** Outputs work the same
for shell and k8s tasks: key=value lines in the file `$CONDUCTOR_OUTPUT`
names. For a pod that file is `/dev/termination-log`, which the kubelet keeps
after the container exits (up to 4 KB), so outputs need no sidecar, volume
or log parsing.

**4. Autoscaling reads a SQL function, not Prometheus.** KEDA's postgresql
scaler runs `SELECT conductor_slot_demand(labels, exclusive)`: tasks a pool
could run that are due, plus tasks running on it. The query lives in a
migration, versioned with the schema, so the autoscaler's settings never
depend on table layout, and `SECURITY DEFINER` lets a role with no table
access call it. A Prometheus-based scaler would make Prometheus a dependency
of scaling.

**5. Pools can be dedicated.** A worker may name exclusive labels
(`CONDUCTOR_WORKER_EXCLUSIVE=pool`) that a task must require to run there,
like a Kubernetes taint. Without it, a GPU pool or a pool that scales from
zero would also take every plain task, and the autoscaler would scale it up
for them. Exclusive labels travel in the heartbeat and are checked in both
the coordinator's worker choice and the pick query.

**6. Worker identity includes the pod name.** Worker IDs were a hash of the
advertised address. Kubernetes reuses pod IPs, so a new pod could inherit a
dead one's ID, and with it the tasks the coordinator still thought it was
running. The ID now hashes the host name too.

**7. The chart is secure by default and keeps its secrets.** Mutual TLS is
on with a generated CA; the cluster token, API key, certificates and dev
database password are generated on install and read back (`lookup`) on
upgrades. Pods run non-root with read-only root filesystems, and each
component accepts connections only from those that need it
(NetworkPolicies). Tools that render with `helm template`, like Argo CD,
can't look anything up, so with them secrets come from outside the chart
(`auth.existingSecret`, `tls.existingSecret`); the Argo CD example uses
cert-manager.

**8. The operator is a separate module and talks to the HTTP API.** It never
touches the database: it is just another API client, with the same
validation and auth as `conductorctl`. Its own Go module keeps
controller-runtime out of the engine's binary. A WorkflowRun starts with the
resource's UID as the idempotency key, so a lost status update can't start a
second run. Resources map to the Conductor namespace named after their
Kubernetes namespace (created on demand), so same-named resources in two
Kubernetes namespaces never overwrite each other. Deleting a Workflow keeps its definition's versions, since past
runs refer to them; deleting a Schedule deletes it; deleting an unfinished
WorkflowRun cancels it.

## Consequences

- The e2e suite runs on Compose and on kind through one platform interface;
  only the network-partition and dev-mode tests are Compose-only. CI runs it
  on kind with dev Postgres and with CloudNativePG.
- Rolling upgrades lose nothing: coordinators surge before stopping (a
  standby takes over), workers drain for `shutdownTimeoutSeconds`, and the
  API's preStop delay lets endpoints update before it stops accepting
  requests.
- Not done: Job-based tasks, NetworkPolicy egress rules (workers' http
  tasks may call anything, and Postgres may be external), and a Prometheus
  Operator alternative to KEDA.
