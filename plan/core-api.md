# Core API Implementation Plan (Mint)

## 1. Summary

- Цель: реализовать `core-api` как внешний REST/BFF фасад для desktop frontend и как runtime для bounded contexts `identity`, `profile`, `workspace`.
- Transport matrix:
  - `frontend -> core-api`: REST.
  - `core-api <-> backend services`: gRPC для синхронных query/orchestration вызовов.
  - Write-команды: только через Kafka.
- В `infrastructure` каждого контекста внутри `core-api` использовать обязательное разделение на `in/` и `out/` адаптеры.
- Политика зависимостей: latest stable only, явная фиксация в `go.mod`/`go.sum`.

## 2. Service Boundaries and Responsibilities

- `core-api` владеет доменной логикой и write/read-моделями контекстов:
  - `identity`
  - `profile`
  - `workspace`
- `core-api` не принимает на себя доменную логику `rtc`, `messaging`, `realtime`; для них он выступает как REST/BFF вход и publisher команд/consumer query результатов.
- Для внешнего клиента `core-api` - единая точка входа в frontend-контур.
- Для межсервисной синхронной коммуникации `core-api` использует и предоставляет gRPC контракты.

## 3. Infrastructure Split (`internal/<context>/infrastructure`)

### 3.1. In adapters

- `in/rest/http`:
  - публичные REST endpoints для frontend (auth/profile/workspace + BFF endpoints к другим сервисам).
- `in/grpc/server`:
  - межсервисные query методы для identity/profile/workspace данных.
- `in/kafka/consumer`:
  - command consumers для `identity/profile/workspace` write-side.
- `in/scheduler` (при необходимости):
  - фоновые задачи relay/projector/cleanup.

### 3.2. Out adapters

- `out/kafka/producer`:
  - публикация команд в соответствующие topics;
  - публикация доменных событий через outbox relay.
- `out/repository/scylla`:
  - write/read-хранилища identity/profile/workspace.
- `out/repository/redis`:
  - ephemeral/session/cache-данные.
- `out/grpc/clients`:
  - клиенты к `rtc-api`, `messaging-api`, `realtime-api` для BFF query aggregation.
- `out/external`:
  - адаптеры внешних систем (если появятся в зоне `core-api`).

## 4. Data and Flow Contracts

- Стандартный envelope для команд/событий: `command_id`, `correlation_id`, `causation_id`, `message_id`, `occurred_at`, `actor_id`, `schema_version` + контекстные ключи (`workspace_id`, `channel_id` и т.д.).
- Идемпотентность обязательна для всех consumers/projectors.
- Partition key выбирается по агрегату (`account_id`, `user_id`, `workspace_id`, `channel_id`).

### 4.1. Write flow (own contexts)

1. Frontend отправляет REST команду в `core-api`.
2. `core-api` выполняет auth + минимальную синхронную валидацию + назначает `command_id`.
3. `core-api` публикует команду в Kafka topic соответствующего контекста.
4. Consumer `core-api` обрабатывает команду в application layer.
5. Изменяется write model и фиксируется outbox запись в той же единице надежности.
6. Outbox relay публикует доменные/интеграционные события.
7. Projectors обновляют read models.

### 4.2. Write flow (foreign contexts через core)

1. Frontend отправляет REST команду в `core-api` (например RTC/Messaging).
2. `core-api` выполняет auth + контрактную валидацию (без бизнес-инвариантов чужого контекста).
3. `core-api` публикует команду в Kafka topic целевого сервиса.
4. Целевой сервис выполняет write-side обработку.

### 4.3. Read flow (BFF aggregation)

1. Frontend делает REST query в `core-api`.
2. `core-api` читает собственные read models напрямую.
3. При необходимости `core-api` делает gRPC query в `rtc/messaging/realtime`.
4. `core-api` агрегирует ответ в frontend-friendly DTO.

## 5. Public APIs / Interfaces

- Внешний публичный API: REST только у `core-api`.
- Межсервисный sync API: gRPC.
- Базовые REST-группы (MVP):
  - `auth` (register/login/refresh/logout/session)
  - `profile` (get/update, privacy, relationships)
  - `workspace` (create/update/list/members/channels/roles/permissions)
  - `bff` endpoints для aggregated views (home/sidebar/workspace bootstrap/state).
- gRPC server (MVP) для межсервисных read use-cases:
  - `GetAccountAuthState`
  - `GetProfile`
  - `GetWorkspace`
  - `ResolvePermissionsForUser`
- gRPC clients (MVP) для `core-api`:
  - `rtc-api` queries
  - `messaging-api` queries
  - `realtime-api` queries

## 6. Implementation Milestones

### Milestone 1: Bootstrap

- `cmd/core-api/main.go` как composition root.
- Конфиг, slog, graceful shutdown, health/readiness endpoints.
- Инициализация Kafka/Scylla/Redis/gRPC clients/REST server.

### Milestone 2: Owned contexts write/read runtime

- Application handlers и consumers для `identity/profile/workspace`.
- Репозитории и read-model projectors.
- Outbox relay + idempotency store.

### Milestone 3: REST/BFF transport

- REST command endpoints (accept+publish pattern, `202 Accepted` где уместно).
- REST query endpoints (direct read model + DTO mapping).
- Correlation/trace propagation для всех запросов.

### Milestone 4: gRPC inter-service layer

- gRPC server для выдачи identity/profile/workspace query данных другим сервисам.
- gRPC clients к `rtc/messaging/realtime` для агрегированных frontend-ответов.

### Milestone 5: Hardening

- AuthN/AuthZ middleware.
- Rate limiting, request validation, timeout/retry/circuit-breaker policy для gRPC clients.
- Метрики, структурированные логи, трассировка.

## 7. Test and Review Checklist

- Документационная консистентность:
  - `AGENTS.md`, `tz.md`, `plan/core-api.md`, `plan/rtc.md` согласованы по transport matrix.
- Unit tests:
  - table-driven + `t.Run`, `testify`, `mockery`.
  - командные handler'ы и domain-инварианты.
- Integration tests:
  - Kafka publish/consume, outbox relay, idempotency, projectors.
  - Scylla/Redis adapters.
  - gRPC clients/server contracts.
- Blackbox/e2e сценарии:
  - `frontend -> core REST -> Kafka command -> core consumer` (identity/profile/workspace).
  - `frontend -> core REST -> Kafka command -> target service` (rtc/messaging).
  - `frontend -> core REST query -> core read + grpc aggregation`.

## 8. Dependency Policy

- Использовать только стабильные версии библиотек (без alpha/beta/rc/pre-release).
- Проверять latest stable по официальным release notes/changelog перед обновлением.
- Фиксировать версии зависимостей в `go.mod`/`go.sum`.
- После изменения зависимостей запускать линтеры и тесты.
