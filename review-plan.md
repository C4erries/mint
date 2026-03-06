# Review Plan

Документ фиксирует план поэтапного ревью кодовой базы на 5 частей.
Сейчас это только разбиение по областям и правила прохода, без детального чтения каждого файла.

## Как выполняем ревью в следующих итерациях

1. Берем одну часть по номеру.
2. Идем по коду этой части и ищем:
- странности в логике;
- потенциальные баги и регрессии;
- переусложнение и избыточные паттерны;
- неочевидные архитектурные решения и нарушения границ слоев;
- места, которые стоит упростить (без "production-borsch" там, где это не нужно).
3. Базовые и безопасные улучшения правим сразу в коде.
4. Архитектурные спорные вопросы не ломаем сходу: фиксируем в этом файле в блоке соответствующей части.
5. После прохода части меняем статус на `reviewed` и дописываем:
- что исправлено сразу;
- что требует отдельного решения/рефакторинга.

## Часть 1: Core in-adapters и внешний API

Статус: `reviewed`

Scope:
- `cmd/core/`
- `internal/core/infrastructure/in/grpc/permission_server/`
- `internal/core/infrastructure/in/http/handlers/`
- `internal/core/infrastructure/in/http/middleware/`
- `internal/core/infrastructure/in/http/router/`
- `api/permission/v1/`

Фокус:
- корректность REST/gRPC edge-логики;
- валидация входа, маппинг DTO, коды ошибок;
- отсутствие доменной логики в transport.

Результаты ревью:
- Быстрые фиксы:
  - `internal/core/infrastructure/in/grpc/permission_server/server.go`: добавлена защита от `nil` permission service (вместо потенциальной panic теперь возвращается `codes.Internal`).
  - `internal/core/infrastructure/in/grpc/permission_server/server_test.go`: добавлен тест на сценарий `service not configured`.
  - `internal/core/infrastructure/in/http/handlers/auth/handler.go`: проверка пустого тела в logout стала корректнее через `errors.Is(err, io.EOF)` (с сохранением совместимости).
  - `internal/core/infrastructure/in/http/handlers/workspace/handler.go`: добавлена ранняя валидация `channel kind` (`text|voice`) в edge-слое.
  - `internal/core/infrastructure/in/http/handlers/workspace/handler.go`: добавлена `nil`-защита publisher перед публикацией команды.
  - `internal/core/infrastructure/in/http/handlers/rtc/handler.go`: добавлены `nil`-защиты для publisher/query client; query endpoints возвращают `503`, если клиент не сконфигурирован.
- Требует решения/упрощения:
  - `internal/core/infrastructure/in/http/handlers/workspace/handler.go`: endpoint `POST /workspaces/:workspace_id/members/:user_action` с форматом `user_id:join` в path выглядит хрупко и неочевидно; упростить контракт (например, `.../members/:user_id/join`).
  - `internal/core/infrastructure/in/http/handlers/auth/handler.go`: `clientIP` доверяет `X-Forwarded-For` без явной trusted-proxy политики; определить единый подход (доверять только известным proxy или использовать `c.ClientIP()` + конфиг gin trusted proxies).
  - `internal/core/infrastructure/in/http/router/router.go`: при `AuthParser == nil` protected routes остаются без auth middleware; решить, это допустимый dev-mode или misconfiguration, и зафиксировать fail-fast политику.
  - `internal/core/infrastructure/in/http/handlers/auth/`, `.../workspace/`, `.../middleware/`, `.../router/`: низкое покрытие unit-тестами для edge-ошибок и маппинга HTTP кодов.

## Часть 2: Core out-adapters, orchestration и runtime

Статус: `reviewed`

Scope:
- `internal/core/infrastructure/out/grpc/`
- `internal/core/infrastructure/out/kafka/`
- `internal/core/infrastructure/config/`
- `internal/core/infrastructure/di/`
- `deploy/docker-compose/core.yml`

Фокус:
- надежность Kafka/gRPC интеграций;
- idempotency/ретраи/ошибки;
- чистота DI и границы между in/out;
- упрощение orchestration, где есть лишняя сложность.

Результаты ревью:
- Быстрые фиксы:
  - `internal/core/infrastructure/out/grpc/clients/rtcquery/client.go`: добавлен timeout на `grpc dial` (startup больше не может зависнуть бесконечно на подключении к RTC gRPC).
  - `internal/core/infrastructure/out/kafka/producer/producer.go`: добавлены защиты от `nil` producer/writer и корректная проверка `topic` с `TrimSpace`.
  - `internal/core/infrastructure/config/config_validate.go`: добавлена валидация `MINT_REDIS_DB >= 0`.
  - `deploy/docker-compose/core.yml`: исправлен healthcheck Redpanda (`rpk cluster health --exit-when-healthy`), включен `auto_create_topics`, для Scylla добавлены dev-параметры (`--developer-mode`, `--reactor-backend=epoll`) для более стабильного локального старта.
- Требует решения/упрощения:
  - `internal/core/infrastructure/out/grpc/clients/rtcquery/client.go`: отсутствуют unit-тесты на retry/backoff/timeout политику клиента.
  - `internal/core/infrastructure/out/kafka/producer/producer.go`: отсутствуют unit-тесты на edge-cases publish/close (закрытый producer, пустой topic, ошибки writer).
  - `internal/core/infrastructure/di/container.go`: один крупный composition-root (много ответственности в одной функции); по мере роста сервиса стоит разнести сборку по небольшим builder-функциям без усложнения абстракций.
  - `deploy/docker-compose/core.yml`: фиксированные host-порты повышают шанс конфликтов при параллельных контурах; желательно параметризовать порты env-переменными по аналогии с `rtc.yml`.
  - `deploy/docker-compose/core.yml`: `go run` в контейнерах годится для dev, но сильно замедляет cold start из-за `go mod download`; для стабильного локального цикла лучше отдельный dev-образ/кэшируемая сборка.

## Часть 3: RTC domain + application

Статус: `reviewed`

Scope:
- `internal/rtc/domain/`
- `internal/rtc/application/`
- `internal/rtc/application/mocks/`

Фокус:
- инварианты домена и корректность use-case;
- семантика команд/событий CQRS;
- избыточная сложность в application-слое;
- читаемость и тестопригодность.

Результаты ревью:
- Быстрые фиксы:
  - `internal/rtc/application/command_service.go`: в `NewCommandService` добавлена валидация `nil`-зависимостей (`rooms/grants/permissions/livekit`), чтобы исключить поздние `panic` в runtime.
  - `internal/rtc/application/command_service_token.go`: добавлен fallback для `ExpiresAt`, если LiveKit вернул нулевое значение (`expires_at = issued_at + ttl`).
  - `internal/rtc/application/command_service_test.go`: добавлены регрессионные тесты:
    - проверка валидации зависимостей конструктора (`TestNewCommandService_ValidateDependencies`);
    - проверка fallback логики `ExpiresAt` (`TestCommandService_IssueRtcToken_FallbackExpiresAtWhenLiveKitReturnsZero`).
  - `internal/rtc/application/command_service_test.go`: убраны мелкие тестовые шероховатости в mock-closure (unused args), чтобы тесты были чище.
- Требует решения/упрощения:
  - `internal/rtc/application/command_service_state.go`: `TerminateVoiceSession` публикует `EventVoiceChannelLeft` c payload reason=`session_terminated`; стоит решить, оставить ли это как overload существующего события или ввести отдельное событие завершения сессии для более чистой семантики стрима.
  - `internal/rtc/application/queries.go` + `internal/rtc/application/query_service.go`: read DTO `RtcTokenGrantStatus` возвращает сырое поле `Token`; нужно зафиксировать политику (это допустимо по контракту или токен должен быть недоступен в query API).
  - `internal/rtc/application/command_service_token.go`: метод `IssueRtcToken` остается перегруженным ветвлениями idempotency/issue/save/reload; имеет смысл позже упростить через небольшие приватные шаги без изменения поведения.

## Часть 4: RTC infrastructure и runtime

Статус: `reviewed`

Scope:
- `cmd/rtc-api/`
- `internal/rtc/infrastructure/config/`
- `internal/rtc/infrastructure/di/`
- `internal/rtc/infrastructure/in/`
- `internal/rtc/infrastructure/out/`
- `internal/rtc/smoke/`
- `deploy/docker-compose/rtc.yml`

Фокус:
- transport reliability (Kafka consumer ack/commit, DLQ, webhook flow);
- корректность in/out adapter границ;
- устойчивость к ошибкам интеграций (Scylla/Redis/LiveKit/gRPC);
- избыточные паттерны и потенциальное упрощение.

Результаты ревью:
- Быстрые фиксы:
  - `internal/rtc/infrastructure/in/grpc/query_server/server.go`: добавлена защита от `nil` query service; вместо panic теперь возвращается `codes.Unavailable`.
  - `internal/rtc/infrastructure/in/http/livekit_webhook/handler_events.go`: для поддерживаемых webhook event types добавлена защита от `nil` command service (controlled error вместо panic).
  - `internal/rtc/infrastructure/in/kafka/command_consumer/consumer.go`: добавлена ранняя проверка `reader` в `Run`, чтобы misconfiguration не приводила к panic.
  - `internal/rtc/infrastructure/in/kafka/command_consumer/kafka_reader.go`: нормализация `Topic`/`GroupID` через `TrimSpace` перед валидацией и созданием `kafka.ReaderConfig`.
  - `internal/rtc/infrastructure/out/kafka/event_publisher/kafka_producer.go`: в `Publish` добавлена trim-валидация `topic`.
  - `internal/rtc/infrastructure/out/kafka/event_publisher/command_dlq_publisher.go`: DLQ topic теперь сохраняется в нормализованном (`TrimSpace`) виде.
  - `internal/rtc/infrastructure/out/kafka/event_publisher/publisher.go`: добавлена защита от `nil` producer в `Publish`.
  - `internal/rtc/infrastructure/config/config_validate.go`: добавлена валидация `MINT_REDIS_DB >= 0`.
- Требует решения/упрощения:
  - `internal/rtc/infrastructure/out/repository/scylla/store_query.go`: `ListUnpublished` использует secondary index + `ALLOW FILTERING` по `published`; для роста нагрузки это слабое место outbox polling (нужна модель без filtering по bool-флагу).
  - `internal/rtc/infrastructure/di/container.go`: readiness сейчас проверяет только Scylla/Redis; отсутствуют проверки доступности Kafka producer path, permission gRPC и LiveKit, из-за чего сервис может быть "ready", но не способен обрабатывать критичные потоки.
  - `internal/rtc/smoke/rtc_smoke_test.go`: smoke тесты под build tag `smoke` и не участвуют в обычном `go test`/`make test`; стоит закрепить отдельный CI/dev gate, чтобы не терять проверку transport-потоков.

## Часть 5: Identity, Workspace и общая инженерная обвязка

Статус: `reviewed`

Scope:
- `internal/identity/`
- `internal/workspace/`
- `pkg/`
- `api/rtc/v1/`
- `Makefile`
- `.golangci.yml`
- `mockery.yml`
- `go.mod`, `go.sum`

Фокус:
- согласованность подходов между bounded contexts;
- избыточные абстракции и шаблоны;
- качество инфраструктурных и тестовых настроек;
- возможность упростить поддержку и локальную разработку.

Результаты ревью:
- Быстрые фиксы:
  - `internal/workspace/infrastructure/in/kafka/command_consumer/consumer.go`: добавлены ранние проверки `nil` для consumer/reader/command service, чтобы misconfiguration не приводила к panic.
  - `internal/workspace/infrastructure/in/kafka/command_consumer/kafka_reader.go`: нормализация `Topic` и `GroupID` через `TrimSpace` перед валидацией и созданием `kafka.ReaderConfig`.
  - `internal/workspace/infrastructure/out/kafka/event_publisher/publisher.go`: добавлены проверки `nil` producer и пустого topic (с `TrimSpace`) перед публикацией.
  - `internal/identity/application/service.go`: в `Logout` убрано подавление ошибки (`_ = ...`) при `MarkRevoked` refresh token; ошибка теперь возвращается наверх.
  - `internal/identity/application/service_test.go`: добавлен регрессионный тест `TestService_Logout_ReturnsRevocationError`.
  - `Makefile`: исправлен текст help для `proto-check`.
- Требует решения/упрощения:
  - `internal/workspace/infrastructure/in/kafka/command_consumer/consumer.go`: при ошибке `dispatch` сообщение не ack'ается, retry/DLQ отсутствуют — poison-команда может блокировать partition бесконечно.
  - `internal/workspace/infrastructure/out/repository/scylla/store.go`: outbox query использует `ALLOW FILTERING` по `published=false`; это узкое место при росте нагрузки.
  - `go.mod`: прямой dependency `github.com/livekit/protocol` зафиксирован на pseudo-version (`v1.44.1-0...`), что конфликтует с политикой `stable only`.
  - `mockery.yml`: покрывает только `internal/rtc/application`; для `identity/workspace` отсутствует единая конфигурация генерации моков, из-за чего тестовая стратегия между контекстами несогласованна.
  - `internal/workspace/infrastructure/...`: почти нет unit/integration тестов для Kafka/scylla adapters и command consumer (в отличие от RTC), что повышает риск скрытых регрессий.

## Порядок прохода

Рекомендуемый порядок: `1 -> 2 -> 3 -> 4 -> 5`.

## Definition of Done для каждой части

- Статус части = `reviewed`.
- Базовые правки внесены в код.
- В этом файле зафиксированы оставшиеся архитектурные вопросы/упрощения.
