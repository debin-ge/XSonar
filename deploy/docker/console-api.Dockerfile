# syntax=docker/dockerfile:1.7

FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY apps ./apps
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/console-api ./apps/console-api

FROM alpine:3.20

WORKDIR /app

RUN mkdir -p /app/config /app/runtime/logs/console-api

COPY --from=build /out/console-api /app/console-api
COPY deploy/configs/local/console-api.yaml /app/config/console-api.yaml

ENV CONFIG=/app/config/console-api.yaml

EXPOSE 8081

ENTRYPOINT ["/app/console-api"]
