#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

SMOKE_BASE_URL="${SMOKE_BASE_URL:?SMOKE_BASE_URL is required}"
RTC_COMMANDS_TOPIC="${RTC_COMMANDS_TOPIC:-mint.rtc.commands.v1}"
RTC_COMMANDS_DLQ_TOPIC="${RTC_COMMANDS_DLQ_TOPIC:-mint.rtc.commands.dlq.v1}"
LIVEKIT_BASE_URL="${LIVEKIT_BASE_URL:-http://127.0.0.1:${MINT_DEV_LIVEKIT_HTTP_PORT:-7880}}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-420}"
SMOKE_POLL_SECONDS="${SMOKE_POLL_SECONDS:-3}"

health_ready() {
  http_request "GET" "$SMOKE_BASE_URL/healthz"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  [[ "$HTTP_BODY" == "ok" ]]
}

core_ready() {
  http_request "GET" "$SMOKE_BASE_URL/readyz"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  [[ "$(json_value "status" 2>/dev/null || true)" == "ready" ]]
}

log "waiting for /healthz and /readyz"
poll_until "core healthz" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" health_ready || fail "core /healthz is not ready"
poll_until "core readyz" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" core_ready || fail "core /readyz is not ready"

livekit_reachable() {
  http_request "GET" "$LIVEKIT_BASE_URL"
  [[ "$HTTP_STATUS" != "000" && -n "$HTTP_STATUS" ]]
}

log "fail-fast: checking LiveKit reachability"
poll_until "livekit reachable" 60 2 livekit_reachable || fail "livekit is not reachable at $LIVEKIT_BASE_URL"

EMAIL="rtc-smoke-$(new_id email)@example.test"
PASSWORD="SmokePass-$(new_id pass)"
REGISTER_PAYLOAD="$(printf '{"email":%s,"password":%s}' "$(json_string "$EMAIL")" "$(json_string "$PASSWORD")")"

log "auth happy path: register -> login -> me"
http_request "POST" "$SMOKE_BASE_URL/api/v1/auth/register" "$REGISTER_PAYLOAD"
assert_status "201" "register"
ACCOUNT_ID="$(json_value '.account_id')"

http_request "POST" "$SMOKE_BASE_URL/api/v1/auth/login" "$REGISTER_PAYLOAD"
assert_status "200" "login"
ACCESS_TOKEN="$(json_value '.access_token')"
LOGIN_ACCOUNT_ID="$(json_value '.account_id')"
[[ "$LOGIN_ACCOUNT_ID" == "$ACCOUNT_ID" ]] || fail "login account_id mismatch"

http_request "GET" "$SMOKE_BASE_URL/api/v1/auth/me" "" "$ACCESS_TOKEN"
assert_status "200" "auth me"
ME_ACCOUNT_ID="$(json_value '.account_id')"
[[ "$ME_ACCOUNT_ID" == "$ACCOUNT_ID" ]] || fail "me account_id mismatch"

WORKSPACE_NAME="rtc-smoke-workspace-$(new_id ws)"
WORKSPACE_PAYLOAD="$(printf '{"name":%s}' "$(json_string "$WORKSPACE_NAME")")"

log "workspace happy path: create workspace and poll read model"
http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces" "$WORKSPACE_PAYLOAD" "$ACCESS_TOKEN"
assert_status "202" "create workspace command"
WORKSPACE_ID="$(json_value '.workspace_id')"

workspace_visible() {
  local workspace_id
  http_request "GET" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID" "" "$ACCESS_TOKEN"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  workspace_id="$(json_value "id" 2>/dev/null || true)"
  [[ "$workspace_id" == "$WORKSPACE_ID" ]]
}
poll_until "workspace projection" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" workspace_visible || fail "workspace projection was not built"

CHANNEL_NAME="rtc-smoke-voice-$(new_id channel)"
CHANNEL_PAYLOAD="$(printf '{"name":%s,"kind":"voice"}' "$(json_string "$CHANNEL_NAME")")"
http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels" "$CHANNEL_PAYLOAD" "$ACCESS_TOKEN"
assert_status "202" "create voice channel command"
CHANNEL_ID="$(json_value '.channel_id')"

channel_visible() {
  local channel_id
  local channel_kind
  http_request "GET" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID" "" "$ACCESS_TOKEN"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  channel_id="$(json_value "id" 2>/dev/null || true)"
  channel_kind="$(json_value "kind" 2>/dev/null || true)"
  [[ "$channel_id" == "$CHANNEL_ID" && "$channel_kind" == "voice" ]]
}
poll_until "voice channel projection" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" channel_visible || fail "voice channel projection was not built"

log "rtc happy path: join -> state -> binding -> leave"
http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/join" "" "$ACCESS_TOKEN"
assert_status "202" "join voice command"

voice_joined() {
  local active
  local participant_count
  http_request "GET" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/state" "" "$ACCESS_TOKEN"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  active="$(json_value "state.active" 2>/dev/null || true)"
  participant_count="$(json_value "state.participant_count" 2>/dev/null || true)"
  [[ "$active" == "true" && "$participant_count" =~ ^[0-9]+$ && "$participant_count" -ge 1 ]]
}
poll_until "voice state after join" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" voice_joined || fail "voice state did not become active"

voice_binding_visible() {
  local room_id
  http_request "GET" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/binding" "" "$ACCESS_TOKEN"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  room_id="$(json_value "binding.room_id" 2>/dev/null || true)"
  [[ -n "$room_id" && "$room_id" != "null" ]]
}
poll_until "voice binding" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" voice_binding_visible || fail "voice binding is not available"

ISSUE_TOKEN_PAYLOAD='{"ttl_seconds":300,"can_publish":true,"can_subscribe":true}'
http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/token" "$ISSUE_TOKEN_PAYLOAD" "$ACCESS_TOKEN"
assert_status "202" "issue rtc token command"
TOKEN_COMMAND_ID="$(json_value '.command_id')"
TOKEN_ID="$(json_value '.token_id')"
[[ "$TOKEN_ID" == "$TOKEN_COMMAND_ID" ]] || fail "token_id must match command_id for deterministic grant lookup"

token_grant_ready() {
  local grant_token_id
  local grant_token
  http_request "GET" "$SMOKE_BASE_URL/api/v1/rtc/token-grants/$TOKEN_ID" "" "$ACCESS_TOKEN"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  grant_token_id="$(json_value "status.token_id" 2>/dev/null || true)"
  grant_token="$(json_value "status.token" 2>/dev/null || true)"
  [[ "$grant_token_id" == "$TOKEN_ID" && -n "$grant_token" && "$grant_token" != "null" ]]
}
poll_until "rtc token grant status" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" token_grant_ready || fail "rtc token grant status is not ready"

http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/leave" "" "$ACCESS_TOKEN"
assert_status "202" "leave voice command"

voice_left() {
  local active
  local participant_count
  http_request "GET" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/state" "" "$ACCESS_TOKEN"
  LAST_VOICE_LEFT_STATUS="$HTTP_STATUS"
  LAST_VOICE_LEFT_BODY="$HTTP_BODY"
  [[ "$HTTP_STATUS" == "200" ]] || return 1
  active="$(json_value "state.active" 2>/dev/null || echo "false")"
  participant_count="$(json_value "state.participant_count" 2>/dev/null || echo "0")"
  [[ "$active" == "false" && "$participant_count" == "0" ]]
}
poll_until "voice state after leave" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" voice_left || fail "voice state did not become inactive (status=$LAST_VOICE_LEFT_STATUS body=$LAST_VOICE_LEFT_BODY)"

log "poison -> DLQ -> recovery scenario"
POISON_COMMAND_ID="$(new_id poison-command)"
POISON_KEY="$(new_id poison-key)"
POISON_OCCURRED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
POISON_PAYLOAD="$(printf '{"type":"PoisonRtcCommand","meta":{"command_id":%s,"correlation_id":%s,"causation_id":%s,"message_id":%s,"occurred_at":%s,"workspace_id":%s,"channel_id":%s,"room_id":"","actor_id":%s,"schema_version":1},"payload":{"user_id":%s}}' \
  "$(json_string "$POISON_COMMAND_ID")" \
  "$(json_string "$(new_id poison-correlation)")" \
  "$(json_string "$(new_id poison-causation)")" \
  "$(json_string "$(new_id poison-message)")" \
  "$(json_string "$POISON_OCCURRED_AT")" \
  "$(json_string "$WORKSPACE_ID")" \
  "$(json_string "$CHANNEL_ID")" \
  "$(json_string "$ACCOUNT_ID")" \
  "$(json_string "$ACCOUNT_ID")")"

printf '%s\n' "$POISON_PAYLOAD" | compose exec -T redpanda rpk topic produce "$RTC_COMMANDS_TOPIC" -k "$POISON_KEY" >/dev/null

dlq_has_poison() {
  local dlq_dump
  dlq_dump="$(run_with_timeout 5 docker compose -f "$SMOKE_COMPOSE_FILE" exec -T redpanda rpk topic consume "$RTC_COMMANDS_DLQ_TOPIC" -n 20 --offset start -f '%v\n' 2>/dev/null || true)"
  [[ -n "$dlq_dump" ]] || return 1
  grep -F "\"command_id\":\"$POISON_COMMAND_ID\"" >/dev/null <<<"$dlq_dump"
}
poll_until "rtc poison command in DLQ" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" dlq_has_poison || fail "poison command was not found in DLQ"

http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/join" "" "$ACCESS_TOKEN"
assert_status "202" "recovery join voice command"
poll_until "voice state recovery after poison" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" voice_joined || fail "rtc consumer did not recover after poison message"

http_request "POST" "$SMOKE_BASE_URL/api/v1/workspaces/$WORKSPACE_ID/channels/$CHANNEL_ID/voice/leave" "" "$ACCESS_TOKEN"
assert_status "202" "recovery leave voice command"
poll_until "final voice inactive state" "$SMOKE_TIMEOUT_SECONDS" "$SMOKE_POLL_SECONDS" voice_left || fail "final voice state did not become inactive"

log "all smoke checks passed"
