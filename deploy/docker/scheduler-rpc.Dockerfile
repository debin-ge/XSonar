# syntax=docker/dockerfile:1.7

FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY apps ./apps
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/scheduler-rpc ./apps/scheduler-rpc

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/scheduler-rpc

COPY --from=build /out/scheduler-rpc /app/scheduler-rpc
COPY deploy/configs/local/scheduler-rpc.yaml /app/config/scheduler-rpc.yaml

ENV CONFIG=/app/config/scheduler-rpc.yaml

EXPOSE 9004

ENTRYPOINT ["/app/scheduler-rpc"]
