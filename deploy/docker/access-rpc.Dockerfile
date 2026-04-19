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

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/access-rpc ./apps/access-rpc

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/access-rpc

COPY --from=build /out/access-rpc /app/access-rpc
COPY deploy/configs/local/access-rpc.yaml /app/config/access-rpc.yaml

ENV CONFIG=/app/config/access-rpc.yaml

EXPOSE 9001

ENTRYPOINT ["/app/access-rpc"]
