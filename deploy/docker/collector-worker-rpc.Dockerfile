# syntax=docker/dockerfile:1.7

FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY apps ./apps
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/collector-worker-rpc ./apps/collector-worker-rpc

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/collector-worker-rpc /app/runtime/collector

COPY --from=build /out/collector-worker-rpc /app/collector-worker-rpc
COPY deploy/configs/local/collector-worker-rpc.yaml /app/config/collector-worker-rpc.yaml

ENV CONFIG=/app/config/collector-worker-rpc.yaml

EXPOSE 9005

ENTRYPOINT ["/app/collector-worker-rpc"]
