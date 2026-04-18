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

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/gateway-api ./apps/gateway-api

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/gateway-api

COPY --from=build /out/gateway-api /app/gateway-api
COPY deploy/configs/local/gateway-api.yaml /app/config/gateway-api.yaml

ENV CONFIG=/app/config/gateway-api.yaml

EXPOSE 8080

ENTRYPOINT ["/app/gateway-api"]
