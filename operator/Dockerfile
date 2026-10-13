# The operator's image. Build from the repository root (it uses the root
# module's Go client):
#
#   docker build -f operator/Dockerfile -t conductor-operator:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY operator/go.mod operator/go.sum ./operator/
RUN cd operator && go mod download

COPY pkg ./pkg
COPY operator ./operator
ARG VERSION=dev
RUN cd operator && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/conductor-operator .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/conductor-operator /conductor-operator
USER 65532:65532
ENTRYPOINT ["/conductor-operator"]
