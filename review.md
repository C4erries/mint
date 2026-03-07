# Fix Plan Before Test Branch

## Goal

Закрыть блокеры по RTC/core перед первичной выгрузкой в `test` ветку: чтобы voice-flow был реально проверен blackbox smoke, а dev-контур поднимался без ложноположительных успехов.

## Plan

1. Blockers first (`LiveKit + full smoke`)
- Исправить ключи LiveKit в compose-контурах (`core.yml`, `rtc.yml`) на валидный формат `key: secret`, чтобы контейнер не завершался сразу после старта.
- Довести smoke до полного voice-контракта: после `join` добавить проверку `issue token -> token grant status` (с polling), а затем `leave`.
- Добавить явный fail-fast на недоступный/упавший LiveKit в smoke-сценарии, чтобы не получать "зеленый" прогон при сломанном media контуре.

2. Stabilize test runtime (`portable e2e`)
- Сделать запуск `rtc-smoke` воспроизводимым в целевом окружении (минимум Linux CI), без shell-специфичных ловушек.
- В smoke-скриптах явно проверять все обязательные утилиты (`docker`, `curl`, `python`, `timeout` или безопасный fallback), чтобы падение было ранним и понятным.
- Сохранить текущую практику предсоздания Kafka topics до начала сценария, как обязательный шаг.
- Статус: `done`
- Что сделано:
  - В `scripts/e2e/lib/common.sh` добавлены runtime-helpers для portability: `require_python`, `ensure_timeout_support`, `run_with_timeout`.
  - В `scripts/e2e/rtc-smoke.sh` добавлены ранние проверки зависимостей (`docker`, `curl`, `python`) и проверка поддержки таймаутов (с fallback через Python, если `timeout` отсутствует).
  - В `scripts/e2e/scenarios/core_rtc_smoke.sh` блокирующие `rpk topic consume` переведены на `run_with_timeout`, чтобы сценарий не зависал и воспроизводимо работал в Linux CI.
  - Предсоздание Kafka topics оставлено обязательным шагом перед запуском сценариев.

3. Finish release hygiene
- Убрать устаревшее поле `version` из docker-compose файлов.
- Синхронизировать API-контракт (OpenAPI + frontend ожидания) по token-grant flow: однозначный `token_id` и явное поле токена в статусе.
- Обновить краткий runbook запуска smoke (одна команда + где смотреть логи при падении).
- Статус: `done`
- Что сделано:
  - Удалено поле `version` из `deploy/docker-compose/core.yml` и `deploy/docker-compose/rtc.yml`.
  - Контракт token-grant синхронизирован:
    - `POST /api/v1/workspaces/{workspace_id}/channels/{channel_id}/voice/token` теперь возвращает `token_id` (детерминированно равен `command_id`);
    - `GET /api/v1/rtc/token-grants/{token_id}` возвращает явное поле `status.token` вместе с `status.token_id`.
  - Обновлены `api/rtc/v1/query.proto` + сгенерированные pb/grpc файлы, `api/openapi/core-api.yaml` и smoke-сценарий под новый flow.
  - Короткий runbook:
    - запуск: `make rtc-smoke`
    - оставить стек для диагностики: `RTC_SMOKE_AUTO_DOWN=0 make rtc-smoke`
    - логи при падении: `docker compose -f deploy/docker-compose/core.yml logs --no-color --tail=300 core rtc-api redpanda livekit`

## Done Criteria

- LiveKit поднимается стабильно, без ошибки про parse keys.
- Smoke покрывает полный сценарий `auth -> workspace -> voice join -> token flow -> leave` и отдельный `poison -> DLQ -> recovery`.
- Smoke запускается в целевом CI и дает воспроизводимый результат.
- Compose и контрактная документация приведены в консистентное состояние.
