# Review Backlog

Открытые и закрытые пункты после ревью `core + rtc`.

## Part A - Critical (P1)

1. Workspace consumer: poison-команда может стопорить partition
- Где: `internal/workspace/infrastructure/in/kafka/command_consumer/consumer.go`
- Что сделано:
  - Вынесен reusable раннер `poll -> dispatch -> retry -> DLQ -> ack` в `pkg/consumer/runner.go`.
  - Подключён в RTC и Workspace consumer.
  - Для workspace добавлены `ConsumerOptions`, `DeadLetterMessage`, `CommandDLQPublisher`, новые config/env поля и wiring в core DI.
- Статус: `done`

## Part B - High Priority (P2)

1. Семантика terminate-события в RTC
- Где: `internal/rtc/application/command_service_state.go`
- Что сделано:
  - Добавлен новый event type `EventVoiceSessionTerminated`.
  - `TerminateVoiceSession` переключен на публикацию `VoiceSessionTerminated` (без backward compatibility).
  - Добавлен unit-тест на outbox event type.
- Статус: `done`

## Part C - Medium Priority (P3)

1. Mock generation не только для RTC
- Где: `mockery.yml`
- Что сделано: добавлены key interfaces для `identity` и `workspace`.
- Статус: `done`

2. Недостаток edge/infrastructure тестов
- Что сделано:
  - `internal/core/infrastructure/out/kafka/producer/producer_test.go`
  - `internal/core/infrastructure/out/grpc/clients/rtcquery/client_test.go`
  - `pkg/consumer/runner_test.go`
  - `internal/workspace/infrastructure/in/kafka/command_consumer/consumer_test.go`
  - `internal/workspace/infrastructure/in/kafka/command_consumer/kafka_reader_test.go`
  - `internal/workspace/infrastructure/out/kafka/event_publisher/command_dlq_publisher_test.go`
  - `internal/workspace/infrastructure/out/kafka/event_publisher/publisher_test.go`
  - `internal/workspace/infrastructure/out/kafka/event_publisher/outbox_relay_test.go`
  - `internal/core/infrastructure/in/http/handlers/workspace/handler_test.go`
- Статус: `done`

3. RTC smoke в Go-коде (реальные подключения) убрать из стандартного тестового контура
- Что сделано:
  - `scripts/e2e/rtc-smoke.sh` реализован как orchestration blackbox smoke (`docker compose up/down`, trap/cleanup, запуск сценария).
  - Добавлен сценарий `scripts/e2e/scenarios/core_rtc_smoke.sh`:
    - readiness/liveness: `GET /healthz`, `GET /readyz`;
    - auth happy-path: register -> login -> `GET /api/v1/auth/me`;
    - workspace happy-path с eventual consistency polling:
      - `POST /api/v1/workspaces` + poll `GET /api/v1/workspaces/{id}`;
      - `POST /api/v1/workspaces/{id}/channels` (voice) + poll `GET /api/v1/workspaces/{id}/channels/{channel_id}`;
    - rtc happy-path: join -> poll state -> binding -> leave -> финальная проверка state;
    - poison/DLQ/recovery: невалидная команда в `mint.rtc.commands.v1` через `rpk` (redpanda), проверка попадания в `mint.rtc.commands.dlq.v1`, затем валидная REST-команда и подтверждение восстановления обработки.
  - Локальный запуск: `make rtc-smoke` (опционально `RTC_SMOKE_AUTO_DOWN=0 make rtc-smoke` для сохранения поднятого стека).
- Статус: `done`

4. Перегруженный core DI composition root
- Где: `internal/core/infrastructure/di/container.go`
- Что сделано: сборка разбита на небольшие builder-функции (`buildIdentityService`, `buildWorkspaceComponents`, `buildWorkspaceRuntime`, `buildRTCQueryClient`, `buildHTTPServer`, `buildGRPCServer`) с централизованным cleanup.
- Статус: `done`

5. Сложность `IssueRtcToken`
- Где: `internal/rtc/application/command_service_token.go`
- Что сделано: метод разделён на приватные шаги (`resolveTokenTTL`, `ensureParticipantActive`, `loadOrIssueGrant`, `issueAndSaveGrant`, `persistIssuedToken`) без изменения внешнего контракта.
- Статус: `done`

6. Хрупкий HTTP member action contract
- Где: `internal/core/infrastructure/in/http/handlers/workspace/handler.go`
- Что сделано: путь упрощён до явного endpoint `POST /workspaces/:workspace_id/members/:user_id/join`, убран формат `user_id:join`.
- Статус: `done`

7. Dev-контур core можно упростить
- Где: `deploy/docker-compose/core.yml`
- Что сделано: параметризованы host ports через env (`${...:-default}`), добавлены `go` cache volumes для dev цикла.
- Статус: `done`
