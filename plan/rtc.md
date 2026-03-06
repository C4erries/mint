# RTC Implementation Plan (Mint)

## 1. Summary

- Цель: реализовать `rtc-api` как отдельный bounded context с чистым DDD/CQRS.
- Transport matrix:
  - `frontend -> core-api`: REST.
  - `core-api <-> rtc-api`: gRPC (синхронные query/orchestration вызовы).
  - Write-команды: только через Kafka.
- В `infrastructure` RTC обязательное разделение на `in/` и `out/` адаптеры.
- Политика зависимостей: только latest stable релизы с явной фиксацией в `go.mod`/`go.sum`.

## 2. Architecture Decisions

- `core-api` остаётся внешним REST/BFF фасадом для desktop frontend.
- `rtc-api` не открывает публичный REST для фронтенда; внешний клиент попадает в RTC только через `core-api`.
- gRPC используется только для межсервисной синхронной коммуникации.
- Команды RTC (`JoinVoiceChannel`, `LeaveVoiceChannel`, `MuteSelf`, `UnmuteSelf`, `EnableCamera`, `DisableCamera`, `IssueRtcToken`, `TerminateVoiceSession`) публикуются в Kafka.
- `rtc-api` применяет команды в write-model и публикует доменные события через outbox.
- Query-path RTC читается напрямую из read-model и отдается в `core-api` по gRPC.
- LiveKit webhook/callback - это входящий (`in`) адаптер `rtc-api`, а не межсервисный API.

## 3. Infrastructure Split (`internal/rtc/infrastructure`)

### 3.1. In adapters

- `in/grpc/query_server`:
  - `GetVoiceRoomState`
  - `ListVoiceParticipants`
  - `GetVoiceChannelBinding`
  - `GetRtcTokenGrantStatus`
- `in/kafka/command_consumer`:
  - чтение команд RTC из `mint.rtc.commands.v1`.
- `in/http/livekit_webhook`:
  - прием LiveKit callbacks, валидация подписи, нормализация в внутренние команды/события.

### 3.2. Out adapters

- `out/repository/scylla`:
  - write-model агрегатов `VoiceRoom`, `VoiceParticipantSession`, `VoiceChannelBinding`.
- `out/repository/redis`:
  - эфемерные состояния и TTL-данные (`MediaAccessGrant`, connection hints).
- `out/kafka/event_publisher`:
  - публикация событий RTC из outbox в `mint.rtc.events.v1`.
- `out/grpc/clients`:
  - gRPC-клиент для синхронного запроса прав/метаданных (если требуется синхронный cross-service read).
- `out/livekit/client`:
  - выдача access grant/token и операции lifecycle room на стороне LiveKit.

## 4. Data and Flow Contracts

- Обязательный envelope для команд/событий: `command_id`, `correlation_id`, `causation_id`, `message_id`, `occurred_at`, `workspace_id`, `channel_id`, `room_id`, `actor_id`, `schema_version`.
- Partition key для команд/событий RTC: `workspace_id:channel_id` (или `room_id`, если агрегат уже создан).
- Идемпотентность обязательна для всех consumers/projectors.

### 4.1. Write flow

1. Frontend вызывает REST endpoint `core-api`.
2. `core-api` валидирует запрос и публикует RTC-команду в Kafka.
3. `rtc-api` command consumer обрабатывает команду в application layer.
4. Изменяется write-model и фиксируется outbox запись в одной единице надежности.
5. Outbox relay публикует доменное событие RTC в Kafka.
6. Проекторы обновляют read-model; realtime-подписчики получают обновления.

### 4.2. Read flow

1. Frontend вызывает REST query в `core-api`.
2. `core-api` делает gRPC query в `rtc-api`.
3. `rtc-api` query handler читает read-model напрямую (без Kafka в запросном пути).
4. `core-api` возвращает агрегированный REST-ответ клиенту.

### 4.3. LiveKit callback flow

1. LiveKit отправляет webhook в `rtc-api` (`in/http/livekit_webhook`).
2. RTC нормализует transport-событие в доменную форму.
3. Дальше применяется стандартный write/outbox/event flow.

## 5. Public APIs / Interfaces

- Внешний публичный API для frontend: только REST у `core-api`.
- Межсервисный sync API: только gRPC.
- RTC gRPC query methods:
  - `GetVoiceRoomState`
  - `ListVoiceParticipants`
  - `GetVoiceChannelBinding`
  - `GetRtcTokenGrantStatus`
- RTC write-команды не выполняются по gRPC.

## 6. Test and Review Checklist

- Документационная консистентность:
  - `AGENTS.md` и `tz.md` одинаково фиксируют `core REST + inter-service gRPC + write via Kafka`.
  - `infrastructure` описана через `in/out` адаптеры.
- Архитектурные сценарии:
  - `frontend -> core REST -> Kafka command -> rtc consumer`.
  - `frontend query -> core REST -> rtc gRPC query`.
  - `LiveKit webhook -> rtc in-adapter -> outbox -> Kafka events`.
- Качество:
  - unit tests таблицами (`t.Run`), `testify`, `mockery`.
  - integration tests для Kafka/outbox/idempotency.
  - регрессионные тесты для повторной доставки команд и событий.

## 7. Dependency Policy

- Использовать только актуальные stable-версии библиотек на момент изменения.
- Не использовать pre-release версии (alpha/beta/rc) в production-плане.
- Фиксировать версии в `go.mod`/`go.sum`.
- Перед обновлением версии проверять официальный changelog/release notes.
- После обновления запускать линтеры и тесты.
