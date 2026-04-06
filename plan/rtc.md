# RTC Plan (Mint)

## 1. Role

- `rtc-api` - bounded context для voice-state, token grant и RTC-событий.
- Вход:
  - gRPC query server;
  - Kafka command consumer;
  - LiveKit webhook handler.
- Выход:
  - Scylla/Redis repositories;
  - Kafka event publisher/outbox relay;
  - gRPC permission client;
  - LiveKit SDK token client.

## 2. Current Status (March 2026)

- Реализован основной write/read runtime (`join/leave/mute/camera/token`).
- Реализованы gRPC query методы:
  - `GetVoiceRoomState`
  - `ListVoiceParticipants`
  - `GetVoiceChannelBinding`
  - `GetRtcTokenGrantStatus`
- Транспортный smoke расширен до полного happy-path + poison/DLQ/recovery.
- Текущий quality gate:
  - `make test` - green.
  - `make lint` - red (общий техдолг репозитория).

## 3. Alpha/MVP Scope (Must Have)

1. Стабильный media-flow для frontend:
- `core` публикует `IssueRtcToken` команду;
- RTC выдает token grant;
- frontend получает grant status и подключается к LiveKit.

2. Интеграционная надежность:
- smoke/e2e должен стабильно проходить в целевом CI окружении;
- LiveKit, Kafka topics и recovery-path не должны давать ложноположительный green.

3. Контрактная консистентность:
- поддерживать синхронность proto/OpenAPI/handler поведения по `token_id` и token grant status.

## 4. Beta Scope (Should Have)

- Довести webhook hardening и дополнительные security-проверки.
- Расширить тесты на деградационные сценарии (timeouts/retries/load spikes).
- Свести lint-долг по RTC пакетам к green.

## 5. Dependencies and Rules

- Stable-only dependencies.
- Явный пиннинг версий.
- Любые контрактные изменения синхронизировать с `plan.md`, `plan/core-api.md`, `api/rtc/v1/query.proto`, `api/openapi/core-api.yaml`.
