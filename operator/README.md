# Conductor operator

Manage Conductor workflows, runs and schedules as Kubernetes resources, so
they live in Git and ship with `kubectl apply` or Argo CD like everything
else. The operator talks to Conductor's HTTP API; it needs no database
access and no special permissions in Conductor beyond an API key.

| Kind | Short name | What it does |
|---|---|---|
| `Workflow` | `cwf` | Keeps a workflow definition applied. A changed spec becomes a new version; an unchanged one is a no-op. Deleting the resource leaves the definition's versions in Conductor, since past runs refer to them. |
| `WorkflowRun` | `cwr` | Starts one run, exactly once (the resource's UID is the idempotency key), and follows it: `status.phase`, steps, error. It waits if its workflow doesn't exist yet, so resources can be applied in any order. Deleting it cancels an unfinished run (and compensates) unless `keepRunningOnDelete` is set. The spec can't change; create another run instead. |
| `Schedule` | `csch` | Keeps a cron schedule applied (`PUT /v1/schedules/{name}`, which keeps its run history). `suspend: true` pauses it. Deleting the resource deletes the schedule. |

## Install

The chart installs it with `operator.enabled=true`, along with the CRDs
(kept on uninstall, since deleting a CRD deletes every resource of its kind):

```sh
helm upgrade --install conductor deploy/helm/conductor --set operator.enabled=true
```

It uses the chart's bootstrap admin key, so resources may name any Conductor
namespace (`spec.conductorNamespace`; default `operator.defaultNamespace`).

## Example

```yaml
apiVersion: conductor.dev/v1alpha1
kind: Workflow
metadata:
  name: nightly-report
spec:
  definition:          # the same format as `conductorctl workflow apply`
    inputs:
      day: {default: yesterday}
    steps:
      - name: extract
        type: k8s
        k8s: {image: ghcr.io/acme/reports:1.4, args: [extract, "${{ inputs.day }}"], memory: 512Mi}
      - name: publish
        depends_on: [extract]
        run: ./publish.sh "$REPORT"
        env: {REPORT: "${{ steps.extract.outputs.report }}"}
---
apiVersion: conductor.dev/v1alpha1
kind: Schedule
metadata:
  name: nightly-report
spec:
  cron: "0 3 * * *"
  timezone: Europe/Berlin
  workflow: {name: nightly-report}
---
apiVersion: conductor.dev/v1alpha1
kind: WorkflowRun
metadata:
  name: nightly-report-backfill-0901
spec:
  workflow: nightly-report
  input: {day: "2026-09-01"}
```

```console
$ kubectl get cwf,cwr,csch
NAME                                    VERSION   READY   AGE
workflow.conductor.dev/nightly-report   3         True    2d

NAME                                                     WORKFLOW         PHASE       AGE
workflowrun.conductor.dev/nightly-report-backfill-0901   nightly-report   COMPLETED   5m

NAME                                    CRON        SUSPENDED   NEXT RUN               READY
schedule.conductor.dev/nightly-report   0 3 * * *               2026-10-13T01:00:00Z   True
```

Problems show in each resource's `Ready` condition: `Invalid` when Conductor
rejected the spec (fix it; nothing is retried until it changes),
`Unreachable` when its API failed (retried with back-off), `Waiting` when a
run's workflow doesn't exist yet.

## Develop

It's its own Go module, so controller-runtime and client-go stay out of
Conductor's binary. From the repository root:

```sh
make operator            # build and test (fake Kubernetes and Conductor APIs)
make operator-generate   # after changing api/v1alpha1: deepcopy, CRDs, the chart's copies
make e2e-k8s             # the whole suite on kind, operator included
```

Flags: `--conductor-url` (`CONDUCTOR_URL`), `--default-namespace`
(`CONDUCTOR_NAMESPACE`), `--watch-namespaces`, `--poll-interval` (runs,
default 5s), `--resync` (re-apply Workflows and Schedules, default 5m),
`--leader-elect`. `CONDUCTOR_API_KEY` is required.
