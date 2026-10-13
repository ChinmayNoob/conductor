# GitOps with Argo CD

An app-of-apps that installs Conductor and the workflows that run on it,
all from Git:

```
root.yaml                      conductor-root: syncs apps/
apps/secrets.yaml      wave 0  conductor-secrets: the TLS certificate (secrets/, cert-manager)
apps/conductor.yaml    wave 1  conductor: the chart (CloudNativePG, KEDA autoscaling, operator)
apps/workflows.yaml    wave 2  conductor-workflows: Workflow and Schedule resources (workflows/)
health.yaml                    health checks for the operator's resources
```

Needs Argo CD, cert-manager, CloudNativePG and KEDA in the cluster.

```sh
# Health checks, so a failed run or a rejected workflow shows as Degraded.
kubectl patch configmap argocd-cm -n argocd --type merge --patch-file deploy/argocd/health.yaml

# The one secret not in Git: the cluster token and the bootstrap API key.
# (Or create it from a secret manager with External Secrets.)
kubectl create namespace conductor
kubectl create secret generic conductor-auth -n conductor \
  --from-literal=cluster-token=$(openssl rand -hex 32) \
  --from-literal=api-key=cnd_$(openssl rand -hex 32)

kubectl apply -n argocd -f deploy/argocd/root.yaml
```

Then a workflow change is a pull request: edit
`workflows/nightly-report.yaml`, merge, and the operator applies the new
version. Runs already in progress keep the version they started with.

Why the secrets come from outside the chart: Argo CD renders charts with
`helm template`, where the chart can't read back the secrets it generated
on install, so it would make new ones on every sync (new API key, new
certificates). With `auth.existingSecret` and `tls.existingSecret` it
generates nothing. Fork the repository and point `repoURL` at your fork to
make it yours; `targetRevision` pins a branch or tag.
