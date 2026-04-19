#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

normalize_http_schemes='
  | .schemes = ["http"]
  | .paths |= with_entries(
      .value |= with_entries(
        if .key == "get"
          or .key == "put"
          or .key == "post"
          or .key == "delete"
          or .key == "patch"
          or .key == "options"
          or .key == "head"
        then
          .value |= (.schemes = ["http"])
        else
          .
        end
      )
    )
'

console_swagger_filter='del(."x-date")'

gateway_swagger_filter='del(."x-date")'

mkdir -p "$ROOT_DIR/apps/console-api/docs" "$ROOT_DIR/apps/gateway-api/docs"

goctl api swagger \
  --api "$ROOT_DIR/apps/console-api/console.api" \
  --dir "$ROOT_DIR/apps/console-api/docs" \
  --filename swagger

jq "$console_swagger_filter$normalize_http_schemes" \
  "$ROOT_DIR/apps/console-api/docs/swagger.json" > "$TMP_DIR/swagger.json"
mv "$TMP_DIR/swagger.json" "$ROOT_DIR/apps/console-api/docs/swagger.json"

goctl api swagger \
  --api "$ROOT_DIR/apps/gateway-api/gateway.api" \
  --dir "$ROOT_DIR/apps/gateway-api/docs" \
  --filename swagger

jq "$gateway_swagger_filter$normalize_http_schemes" \
  "$ROOT_DIR/apps/gateway-api/docs/swagger.json" > "$TMP_DIR/swagger.json"
mv "$TMP_DIR/swagger.json" "$ROOT_DIR/apps/gateway-api/docs/swagger.json"
