#!/usr/bin/env bash
# Runs Conductor on a local kind cluster, as the Kubernetes end-to-end
# suite expects it:
#
#   ./scripts/kind-up.sh
#   E2E_PLATFORM=kubernetes E2E_API_URL=http://localhost:30081 E2E_API_KEY=$(./scripts/kind-up.sh --api-key) \
#     go test -tags e2e -count=1 -v ./test/e2e/
#
# It creates the cluster (deploy/kind/cluster.yaml) unless it exists, builds
# the image and loads it into the nodes, installs KEDA (worker autoscaling)
# and, with POSTGRES_MODE=cloudnativepg, the CloudNativePG operator, then
# installs the chart with deploy/kind/values-e2e.yaml. Run it again to
# rebuild and upgrade. Delete everything with: kind delete cluster --name conductor
#
# Needs docker, kind, kubectl and helm.
set -euo pipefail

CLUSTER=${KIND_CLUSTER:-conductor}
NAMESPACE=${NAMESPACE:-conductor}
RELEASE=${RELEASE:-conductor}
POSTGRES_MODE=${POSTGRES_MODE:-dev}
KEDA_VERSION=${KEDA_VERSION:-2.18.1}
CNPG_VERSION=${CNPG_VERSION:-1.27.0}
cd "$(dirname "$0")/.."
# The chart names resources after the release if it contains "conductor",
# else <release>-conductor.
FULLNAME=$RELEASE
[[ "$RELEASE" == *conductor* ]] || FULLNAME=$RELEASE-conductor

if [[ "${1:-}" == "--api-key" ]]; then
  kubectl --context "kind-$CLUSTER" get secret -n "$NAMESPACE" "$FULLNAME-auth" \
    -o jsonpath='{.data.api-key}' | base64 -d
  exit 0
fi

step() { printf '\n==> %s\n' "$*"; }

if ! kind get clusters | grep -qx "$CLUSTER"; then
  step "Creating kind cluster $CLUSTER"
  kind create cluster --name "$CLUSTER" --config deploy/kind/cluster.yaml --wait 3m
fi
export KUBECONFIG_CONTEXT="kind-$CLUSTER"
kubectl config use-context "kind-$CLUSTER" >/dev/null

step "Building conductor:dev and conductor-operator:dev and loading them into the cluster"
docker build -q -t conductor:dev . >/dev/null
docker build -q -f operator/Dockerfile -t conductor-operator:dev . >/dev/null
kind load docker-image conductor:dev conductor-operator:dev --name "$CLUSTER"

step "Installing KEDA $KEDA_VERSION"
kubectl apply --server-side --force-conflicts \
  -f "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml" >/dev/null
kubectl wait -n keda --for=condition=Available deployment --all --timeout=5m

extra=()
if [[ "$POSTGRES_MODE" == cloudnativepg ]]; then
  step "Installing CloudNativePG $CNPG_VERSION"
  kubectl apply --server-side --force-conflicts \
    -f "https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v$CNPG_VERSION/cnpg-$CNPG_VERSION.yaml" >/dev/null
  kubectl wait -n cnpg-system --for=condition=Available deployment --all --timeout=5m
  # One instance is plenty on a laptop.
  extra+=(--set postgres.mode=cloudnativepg --set postgres.cloudnativepg.instances=1
          --set postgres.cloudnativepg.storage.size=1Gi)
fi

step "Installing Conductor ($POSTGRES_MODE Postgres) in namespace $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
# A service account k8s tasks in the suite may name.
kubectl create serviceaccount -n "$NAMESPACE" conductor-e2e-reader --dry-run=client -o yaml | kubectl apply -f - >/dev/null
helm upgrade --install "$RELEASE" deploy/helm/conductor -n "$NAMESPACE" \
  -f deploy/kind/values-e2e.yaml "${extra[@]}" --wait --timeout 10m
# New images under the same tag need a restart to be picked up.
kubectl rollout restart -n "$NAMESPACE" deployment -l app.kubernetes.io/instance="$RELEASE" >/dev/null
kubectl rollout status -n "$NAMESPACE" deployment -l app.kubernetes.io/instance="$RELEASE" --timeout=5m

step "Ready: API at http://localhost:30081, key from: $0 --api-key"
