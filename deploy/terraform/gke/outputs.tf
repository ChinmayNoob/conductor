output "get_credentials" {
  description = "Point kubectl at the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.conductor.name} --region ${var.region} --project ${var.project_id}"
}

output "api_key" {
  description = "Print the bootstrap admin API key."
  value       = "kubectl get secret -n conductor conductor-auth -o jsonpath='{.data.api-key}' | base64 -d"
}

output "dashboard" {
  description = "Reach the API and dashboard (then open http://localhost:8081/ui/)."
  value       = "kubectl port-forward -n conductor svc/conductor-api 8081:8081"
}

output "database_instance" {
  description = "The Cloud SQL instance."
  value       = google_sql_database_instance.conductor.connection_name
}
