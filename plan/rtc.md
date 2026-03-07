# RTC Implementation Plan (Mint) - Status Update

## 1. Status Snapshot (March 6, 2026)

- [x] Реализован bounded context `rtc` со слоями `domain/application/infrastructure`.
- [x] В `infrastructure` применено разделение `in/` и `out/` + `di/container.go`.
- [x] Реализован `cmd/rtc-api/main.go` и runtime lifecycle (`Run/Shutdown`).
- [x] Реализован gRPC query server с методами:
  - `GetVoiceRoomState`
  - `ListVoiceParticipants`
  - `GetVoiceChannelBinding`
  - `GetRtcTokenGrantStatus`
- [x] Реализован Kafka command consumer для RTC write-команд.
- [x] Реализован LiveKit webhook handler (`/livekit/webhook`).
- [x] Реализованы out adapters: Scylla store, Redis grant store, Kafka publisher/outbox relay, LiveKit token client.
- [x] `make test` проходит по текущему состоянию репозитория.
- [ ] `make lint` не зелёный (по последнему прогону: 59 issues).
- [ ] Межсервисный permission client пока in-memory stub, без реального gRPC контракта к `core/workspace`.
- [ ] Нет полного e2e контура `frontend -> core REST -> Kafka -> rtc` (это блокируется до завершения `core-api` интеграции).

## 2. Implemented Scope (Fact Check)

### 2.1. Domain

- `VoiceRoom`, `VoiceParticipantSession`, `VoiceChannelBinding`, `MediaAccessGrant` реализованы.
- Доменные ошибки и события реализованы.
- Unit tests домена присутствуют.

### 2.2. Application

- Реализованы command DTO/validation, query DTO/validation, сервисы `CommandService` и `QueryService`.
- Реализованы ключевые use-cases: join/leave/mute/unmute/camera on-off/issue token/terminate session.
- Реализованы application ports и mockery-моки для тестов.

### 2.3. Infrastructure

- `in/grpc/query_server` реализован и подключён в DI.
- `in/kafka/command_consumer` реализован.
- `in/http/livekit_webhook` реализован.
- `out/repository/scylla` реализован (tx, outbox, query-read методы, schema bootstrap).
- `out/repository/redis` реализован для grant-хранилища с TTL.
- `out/kafka/event_publisher` + `outbox_relay` реализованы.
- `out/livekit/client` реализован на `livekit/server-sdk-go/v2`.
- `out/grpc/clients/permission_client` пока stub-модель (in-memory deny map).

### 2.4. Runtime and Config

- `infrastructure/config/config.go` реализован.
- `infrastructure/di/container.go` собирает dependency graph и держит main чистым.
- `cmd/rtc-api/main.go` оставлен как тонкий composition root.

## 3. Remaining Work (Updated)

1. Заменить `out/grpc/clients/permission_client` на реальный gRPC клиент к источнику прав (`core/workspace`) и зафиксировать контракт.
2. Довести integration-покрытие реальных адаптеров (`scylla`, `redis`, `kafka`) в docker-среде.
3. Довести webhook безопасность до production-практик (подпись, timeout, SSRF-safe policy, allowlist/egress control).
4. Закрыть lint-долг до зелёного `make lint`.
5. Синхронизировать финальные Kafka topic names/envelope-конвенции с `core-api` планом.
6. Добавить e2e сценарии cross-service после готовности `core-api` (REST facade + gRPC aggregation).

## 4. Updated Milestones

### Milestone A - Core Integration Contracts

- Реальный permission gRPC client.
- Финальная версия command/event envelope и topic routing.
- Совместимость с `plan/core-api.md` на уровне контрактов.

### Milestone B - Infrastructure Hardening

- Интеграционные тесты с реальными Scylla/Redis/Kafka.
- Улучшения webhook security/reliability.
- Проверка graceful shutdown под нагрузкой consumer+relay.

### Milestone C - Quality Gate

- `make test` стабильно зелёный.
- `make lint` зелёный (без ослабления правил).
- Добавлены регрессионные тесты для найденных дефектов.

## 5. Verification Checklist

- `make test`.
- `make lint`.
- Проверка gRPC query методов через контрактные тесты.
- Проверка Kafka consumer idempotency на повторной доставке.
- Проверка outbox relay на recovery после рестарта.
- Проверка TTL/expiry поведения Redis grant store.

## 6. Dependency Policy

- Использовать только stable-релизы библиотек.
- Не использовать alpha/beta/rc в production-контуре.
- Фиксировать версии в `go.mod`/`go.sum`.
- Перед обновлением зависимостей проверять release notes/changelog.
