# One image for every component: `conductor coordinator|api|worker`.
FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/conductor ./cmd/conductor && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/conductorctl ./cmd/conductorctl

FROM alpine:3.20
# bash for task commands, tzdata for schedules in named time zones.
RUN apk add --no-cache ca-certificates bash tzdata && \
    adduser -D -u 10001 conductor
COPY --from=build /out/ /usr/local/bin/
# Tasks run as this unprivileged user, not root.
USER conductor
WORKDIR /home/conductor
ENTRYPOINT ["conductor"]
