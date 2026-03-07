#!/usr/bin/env bash

log() {
  printf '[rtc-smoke] %s\n' "$*"
}

fail() {
  log "ERROR: $*"
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || fail "required command is missing: $cmd"
}

require_tools() {
  local tool
  for tool in "$@"; do
    require_cmd "$tool"
  done
}

python_exec() {
  if command -v python3 >/dev/null 2>&1; then
    python3 "$@"
    return
  fi

  if command -v python >/dev/null 2>&1; then
    python "$@"
    return
  fi

  fail "required command is missing: python3 (or python)"
}

compose() {
  docker compose -f "$SMOKE_COMPOSE_FILE" "$@"
}

new_id() {
  local prefix="${1:-id}"
  printf '%s-%(%s)T-%d' "$prefix" -1 "$RANDOM"
}

http_request() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  local token="${4:-}"
  local response_file
  local status_code
  local -a curl_args

  response_file="$(mktemp)"
  curl_args=(-s -X "$method" "$url" -o "$response_file" -w "%{http_code}" -H "Accept: application/json")
  if [[ -n "$body" ]]; then
    curl_args+=(-H "Content-Type: application/json" -d "$body")
  fi
  if [[ -n "$token" ]]; then
    curl_args+=(-H "Authorization: Bearer $token")
  fi

  status_code="$(curl "${curl_args[@]}" || true)"
  if [[ -z "$status_code" ]]; then
    HTTP_STATUS="000"
    HTTP_BODY=""
    rm -f "$response_file"
    return 0
  fi

  HTTP_STATUS="$status_code"
  HTTP_BODY="$(cat "$response_file")"
  rm -f "$response_file"
}

assert_status() {
  local expected="$1"
  local context="$2"
  if [[ "$HTTP_STATUS" != "$expected" ]]; then
    fail "$context: expected HTTP $expected, got $HTTP_STATUS. body=$HTTP_BODY"
  fi
}

json_value() {
  local expr="$1"
  HTTP_BODY="$HTTP_BODY" python_exec - "$expr" <<'PY'
import json
import os
import sys

path = [part for part in sys.argv[1].split(".") if part]
payload = os.environ.get("HTTP_BODY", "")
obj = json.loads(payload)

for part in path:
    if isinstance(obj, dict) and part in obj:
        obj = obj[part]
    else:
        raise SystemExit(1)

if isinstance(obj, bool):
    print("true" if obj else "false")
elif obj is None:
    print("null")
else:
    print(obj)
PY
}

json_string() {
  local value="${1:-}"
  python_exec - "$value" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1]))
PY
}

poll_until() {
  local description="$1"
  local timeout_seconds="$2"
  local interval_seconds="$3"
  shift 3

  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if "$@"; then
      return 0
    fi
    sleep "$interval_seconds"
  done

  log "timed out: $description"
  return 1
}
