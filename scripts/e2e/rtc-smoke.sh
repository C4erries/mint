#!/usr/bin/env bash
set -euo pipefail

#
# External blackbox smoke for core + rtc.
#
# Usage:
#   make rtc-smoke
#
# Optional env:
#   RTC_SMOKE_COMPOSE_FILE=deploy/docker-compose/core.yml
#   RTC_SMOKE_BASE_URL=http://127.0.0.1:8091
#   RTC_SMOKE_AUTO_DOWN=1   # set 0 to keep stack running

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

export SMOKE_COMPOSE_FILE="${RTC_SMOKE_COMPOSE_FILE:-deploy/docker-compose/core.yml}"
export SMOKE_BASE_URL="${RTC_SMOKE_BASE_URL:-http://127.0.0.1:${MINT_DEV_CORE_HTTP_PORT:-8091}}"
export RTC_COMMANDS_TOPIC="${RTC_COMMANDS_TOPIC:-mint.rtc.commands.v1}"
export RTC_COMMANDS_DLQ_TOPIC="${RTC_COMMANDS_DLQ_TOPIC:-mint.rtc.commands.dlq.v1}"
WORKSPACE_COMMANDS_TOPIC="${WORKSPACE_COMMANDS_TOPIC:-mint.workspace.commands.v1}"
WORKSPACE_COMMANDS_DLQ_TOPIC="${WORKSPACE_COMMANDS_DLQ_TOPIC:-mint.workspace.commands.dlq.v1}"
WORKSPACE_EVENTS_TOPIC="${WORKSPACE_EVENTS_TOPIC:-mint.workspace.events.v1}"
RTC_EVENTS_TOPIC="${RTC_EVENTS_TOPIC:-mint.rtc.events.v1}"
RTC_SMOKE_AUTO_DOWN="${RTC_SMOKE_AUTO_DOWN:-1}"

on_error() {
  log "smoke failed, dumping compose state"
  compose ps || true
  compose logs --no-color --tail=200 core rtc-api redpanda || true
}

cleanup() {
  if [[ "$RTC_SMOKE_AUTO_DOWN" == "1" ]]; then
    log "stopping stack"
    compose down --remove-orphans
  else
    log "keeping stack up (RTC_SMOKE_AUTO_DOWN=$RTC_SMOKE_AUTO_DOWN)"
  fi
}

trap on_error ERR
trap cleanup EXIT

require_tools docker curl

SMOKE_TOPICS=(
  "$WORKSPACE_COMMANDS_TOPIC"
  "$WORKSPACE_COMMANDS_DLQ_TOPIC"
  "$WORKSPACE_EVENTS_TOPIC"
  "$RTC_COMMANDS_TOPIC"
  "$RTC_COMMANDS_DLQ_TOPIC"
  "$RTC_EVENTS_TOPIC"
)

topics_ready() {
  local list
  local topic
  list="$(compose exec -T redpanda rpk topic list 2>/dev/null || true)"
  [[ -n "$list" ]] || return 1

  for topic in "${SMOKE_TOPICS[@]}"; do
    grep -Eq "(^|[[:space:]])$topic([[:space:]]|$)" <<<"$list" || return 1
  done

  return 0
}

ensure_topics() {
  local topic
  for topic in "${SMOKE_TOPICS[@]}"; do
    compose exec -T redpanda rpk topic create "$topic" -p 1 -r 1 >/dev/null 2>&1 || true
  done
}

log "starting stack using $SMOKE_COMPOSE_FILE"
compose up -d
log "ensuring kafka topics for smoke"
ensure_topics
poll_until "kafka topics ready" 120 3 topics_ready || fail "required kafka topics were not created"

"$SCRIPT_DIR/scenarios/core_rtc_smoke.sh"

log "smoke completed successfully"
