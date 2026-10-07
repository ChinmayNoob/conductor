# Pinned toolchain for regenerating pkg/grpcapi. Used by `make proto`.
FROM golang:1.24-alpine
RUN apk add --no-cache protobuf protobuf-dev && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
