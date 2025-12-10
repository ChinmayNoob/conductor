#!/bin/bash

# Navigate to project root
cd "$(dirname "$0")/../.."

# Generate Go code from proto files
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    pkg/grpcapi/api.proto
