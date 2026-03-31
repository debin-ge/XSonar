# syntax=docker/dockerfile:1.7

FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY apps ./apps
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/policy-rpc ./apps/policy-rpc

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/policy-rpc

COPY --from=build /out/policy-rpc /app/policy-rpc
COPY deploy/configs/local/policy-rpc.yaml /app/config/policy-rpc.yaml

ENV CONFIG=/app/config/policy-rpc.yaml

EXPOSE 9002

ENTRYPOINT ["/app/policy-rpc"]
