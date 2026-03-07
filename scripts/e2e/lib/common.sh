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

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

python_exec() {
  if has_cmd python3; then
    python3 "$@"
    return
  fi

  if has_cmd python; then
    python "$@"
    return
  fi

  fail "required command is missing: python3 (or python)"
}

require_python() {
  python_exec -c "import sys" >/dev/null
}

ensure_timeout_support() {
  if has_cmd timeout; then
    return
  fi

  require_python
  log "'timeout' command is missing, using python timeout fallback"
}

run_with_timeout() {
  local timeout_seconds="$1"
  shift

  if has_cmd timeout; then
    timeout "${timeout_seconds}s" "$@"
    return $?
  fi

  python_exec - "$timeout_seconds" "$@" <<'PY'
import subprocess
import sys

if len(sys.argv) < 3:
    sys.exit(2)

timeout_seconds = float(sys.argv[1])
command = sys.argv[2:]

try:
    result = subprocess.run(command, timeout=timeout_seconds, check=False)
except subprocess.TimeoutExpired:
    sys.exit(124)

sys.exit(result.returncode)
PY
}

compose() {
  docker compose -f "$SMOKE_COMPOSE_FILE" "$@"
}

new_id() {
  local prefix="${1:-id}"
  printf '%s-%s-%d' "$prefix" "$(date +%s)" "$RANDOM"
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
