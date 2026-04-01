#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

mkdir -p "$ROOT_DIR/apps/console-api/docs" "$ROOT_DIR/apps/gateway-api/docs"

CONSOLE_API_TMP="$TMP_DIR/console.api"
cp "$ROOT_DIR/apps/console-api/console.api" "$CONSOLE_API_TMP"
console_rotate_workaround_count="$(
  (grep -Fo '/admin/v1/apps/:id/secret:rotate' "$ROOT_DIR/apps/console-api/console.api" || true) \
    | wc -l \
    | tr -d '[:space:]'
)"
console_rotate_direct_count="$(
  (grep -Fo '/admin/v1/apps/:id/secret/rotate' "$ROOT_DIR/apps/console-api/console.api" || true) \
    | wc -l \
    | tr -d '[:space:]'
)"
if [[ "$console_rotate_workaround_count" -ne 1 ]]; then
  echo "expected exactly one console :rotate route before rewrite, found $console_rotate_workaround_count" >&2
  exit 1
fi
if [[ "$console_rotate_direct_count" -ne 0 ]]; then
  echo "console source already contains /secret/rotate; workaround is no longer needed" >&2
  exit 1
fi
perl -0pi -e 's#/admin/v1/apps/:id/secret:rotate#/admin/v1/apps/:id/secret/rotate#' "$CONSOLE_API_TMP"

goctl api swagger \
  --api "$CONSOLE_API_TMP" \
  --dir "$ROOT_DIR/apps/console-api/docs" \
  --filename swagger

jq '
  if .paths["/admin/v1/apps/{id}/secret/rotate"] then
    .paths["/admin/v1/apps/{id}/secret:rotate"] = .paths["/admin/v1/apps/{id}/secret/rotate"]
    | del(.paths["/admin/v1/apps/{id}/secret/rotate"])
    | if .paths["/admin/v1/apps/{id}/secret:rotate"] then
        del(."x-date")
      else
        error("console swagger rewrite did not produce /admin/v1/apps/{id}/secret:rotate")
      end
  else
    error("expected goctl to generate /admin/v1/apps/{id}/secret/rotate before post-processing")
  end
' "$ROOT_DIR/apps/console-api/docs/swagger.json" > "$TMP_DIR/swagger.json"
mv "$TMP_DIR/swagger.json" "$ROOT_DIR/apps/console-api/docs/swagger.json"

goctl api swagger \
  --api "$ROOT_DIR/apps/gateway-api/gateway.api" \
  --dir "$ROOT_DIR/apps/gateway-api/docs" \
  --filename swagger

jq 'del(."x-date")' \
  "$ROOT_DIR/apps/gateway-api/docs/swagger.json" > "$TMP_DIR/swagger.json"
mv "$TMP_DIR/swagger.json" "$ROOT_DIR/apps/gateway-api/docs/swagger.json"
