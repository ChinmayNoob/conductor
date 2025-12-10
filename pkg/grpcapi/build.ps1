# PowerShell script to generate Go code from proto files
# Run from project root: .\pkg\grpcapi\build.ps1

# Navigate to project root
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $scriptPath "../..")

# Generate Go code from proto files
protoc --go_out=. --go_opt=paths=source_relative `
    --go-grpc_out=. --go-grpc_opt=paths=source_relative `
    pkg/grpcapi/api.proto

Write-Host "Proto files generated successfully!" -ForegroundColor Green

