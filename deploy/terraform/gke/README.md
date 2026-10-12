# Conductor on Google Cloud (GKE + Cloud SQL)

A reference setup for running Conductor in production on one managed cloud:

| Piece | What | Why |
|---|---|---|
| GKE Autopilot, private nodes | the cluster | Google runs the nodes; Dataplane V2 enforces the chart's NetworkPolicies |
| Cloud NAT | egress for private nodes | image pulls and `http` tasks |
| Cloud SQL Postgres 16, regional, private IP only | the database (Conductor's only dependency) | a standby in another zone, backups with point-in-time recovery, TLS required |
| KEDA (Helm) | worker autoscaling | the default pool scales on queue depth, down to zero when idle |
| Conductor (the chart in `../../helm/conductor`) | the engine | 2 coordinators, the API with an HPA, workers, k8s tasks, the operator |
| Managed Service for Prometheus | monitoring | a `PodMonitoring` for every Conductor pod; query in Cloud Monitoring or Grafana |

```sh
cd deploy/terraform/gke
terraform init
terraform apply -var project_id=my-project \
  -var conductor_image=europe-west1-docker.pkg.dev/my-project/conductor/conductor \
  -var operator_image=europe-west1-docker.pkg.dev/my-project/conductor/conductor-operator \
  -var image_tag=$(git rev-parse --short HEAD)
$(terraform output -raw get_credentials)
$(terraform output -raw dashboard) &
```

Until releases publish images (Phase 7), build and push your own:

```sh
docker build -t europe-west1-docker.pkg.dev/my-project/conductor/conductor:TAG .
docker build -f operator/Dockerfile -t europe-west1-docker.pkg.dev/my-project/conductor/conductor-operator:TAG .
docker push ...   # both
```

Notes:

- **It costs money.** The regional Cloud SQL instance is the bulk of it;
  set `database_high_availability = false` and a smaller `database_tier`
  for a test setup.
- **Deletion protection** is on for the cluster and the database. To tear
  down: `terraform apply -var deletion_protection=false`, then
  `terraform destroy`.
- **State holds secrets** (the database password). Use a remote backend
  with encryption, e.g. a GCS bucket, not local state.
- **The control plane** has a public endpoint; restrict it with
  `api_authorized_networks`.
- Expose the API with an Ingress (`api.ingress` in the chart) or an
  internal load balancer; it's ClusterIP by default.
- With Managed Prometheus, the metrics' `namespace` label (the Conductor
  namespace) arrives as `exported_namespace`, because GMP reserves
  `namespace` for the Kubernetes one.
