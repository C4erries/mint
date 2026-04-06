# Mint Unified Plan (Alpha/MVP -> Beta)

## 1. Можно ли уже связывать backend и frontend?

Да, уже можно начинать интеграцию и "тыкать" продукт end-to-end.

Что уже достаточно для старта:
- есть рабочие backend сервисы `core` и `rtc`;
- есть контракты REST/gRPC и OpenAPI;
- `make test` проходит;
- есть blackbox smoke-контур для ключевых backend-сценариев.

Ограничение на текущем этапе:
- `make lint` пока красный (это не стоп для первого alpha/MVP, но стоп для beta quality gate).

## 2. Как правильно связать репозитории

Для ближайшей итерации:
1. Делать frontend в отдельном репо.
2. Подключить его как submodule (или держать два репо рядом и поднять единым compose).
3. Использовать общий docker compose orchestration для локального запуска:
- infra (`redpanda`, `redis`, `scylla`, `livekit`);
- backend (`core`, `rtc`);
- frontend (dev runtime по необходимости).

Это нормальный production-путь: frontend ходит в `core-api` и напрямую в LiveKit (SDK/WebRTC) по токену, полученному через `core-api`.

## 3. Что осталось до первой alpha/MVP

1. Стабилизировать интеграционный цикл frontend + backend:
- единая команда запуска dev-контура;
- подтвержденный flow `auth -> workspace -> join -> issue token -> grant status -> livekit connect`.

2. Закрыть обязательный integration gate:
- smoke/e2e стабильно проходит в CI (Linux runner);
- документация запуска и диагностики в актуальном состоянии.

3. Подтвердить контрактную синхронизацию:
- OpenAPI и frontend-клиент в одном состоянии;
- token-flow без ручных эвристик на стороне frontend.

Оценка до alpha/MVP: примерно 5-8 рабочих дней при текущем темпе и фокусе на интеграции.

## 4. Что осталось до beta

1. Довести quality gate:
- `make lint` -> green;
- стабилизировать style/security линтеры без регрессий.

2. Усилить надежность:
- больше e2e на сбои/восстановление;
- дополнительные эксплуатационные проверки (timeouts/retries/observability).

3. Подчистить UX/security долг frontend:
- более надежное хранение токенов;
- обработка reconnect/error paths.

Оценка после alpha до beta: еще 1-2 недели.

## 5. Сервисные планы

- [Core API Plan](plan/core-api.md)
- [RTC Plan](plan/rtc.md)
- [Frontend Plan](plan/frontend.md)
- [Fix Plan Before Test Branch](review.md)
