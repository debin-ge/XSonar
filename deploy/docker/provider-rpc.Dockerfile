# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.25 AS build

ARG TARGETOS=linux
ARG TARGETARCH

WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY apps ./apps
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/provider-rpc ./apps/provider-rpc

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/provider-rpc

COPY --from=build /out/provider-rpc /app/provider-rpc
COPY deploy/configs/local/provider-rpc.yaml /app/config/provider-rpc.yaml

ENV CONFIG=/app/config/provider-rpc.yaml

EXPOSE 9003

ENTRYPOINT ["/app/provider-rpc"]
