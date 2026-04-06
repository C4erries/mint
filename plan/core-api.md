# Core API Plan (Mint)

## 1. Role

- `core-api` - единая внешняя REST/BFF точка входа для frontend.
- Межсервисные синхронные вызовы - только gRPC.
- Write-path - через Kafka, read-path - прямые query/read модели.

## 2. Current Status (March 2026)

- Реализованы базовые REST-группы: `auth`, `workspace`, `rtc`.
- Реализован publish команд в Kafka и query интеграция с RTC через gRPC client.
- Текущий quality gate:
  - `make test` - green.
  - `make lint` - red (техдолг, переносим в beta gate).

## 3. Alpha/MVP Scope (Must Have)

1. Зафиксировать стабильный контракт с frontend:
- поддерживать актуальный OpenAPI (`api/openapi/core-api.yaml`);
- не ломать flow `issue rtc token -> token grant status`.

2. Закрыть интеграционный контур с frontend:
- гарантировать воспроизводимый local/dev запуск `core + rtc + infra + livekit`;
- завершить smoke/e2e сценарии по цепочке `frontend-like REST -> core -> kafka/grpc -> rtc`.

3. Минимальная эксплуатационная готовность:
- health/readiness, structured logs, корректный graceful shutdown;
- документированный runbook запуска для интеграции фронта.

## 4. Beta Scope (Should Have)

- Довести `make lint` до green.
- Ужесточить security/observability (rate limits, trace propagation, retry/timeouts policy review).
- Расширить e2e-покрытие под реальные frontend-сценарии (рефреш токена, восстановление после ошибок, деградации внешних зависимостей).

## 5. Dependencies and Rules

- Только stable-релизы библиотек.
- Явный пиннинг версий.
- Любые API-изменения синхронизировать с `plan.md` и `api/openapi/core-api.yaml`.
