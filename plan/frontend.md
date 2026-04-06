# Frontend Plan (Desktop: React + Electron)

## 1. Role

- Отдельный frontend-репозиторий для desktop-клиента.
- Архитектурно frontend работает:
  - с `core-api` по REST (control plane);
  - с LiveKit напрямую по SDK/WebRTC (media plane).

## 2. Integration Model

- Допустимо и практично вести frontend в отдельном репо.
- Для совместной разработки можно использовать git submodule:
  - backend repo + `frontend/` submodule;
  - общий docker compose orchestration для локального запуска.
- Альтернатива (чистее в долгую): отдельный orchestration repo с двумя submodule (`mint-backend`, `mint-frontend`).

## 3. Alpha/MVP Scope (Must Have)

1. Базовый UI-flow:
- auth (register/login/me);
- workspace/channel bootstrap;
- voice join/leave.

2. RTC-flow:
- `issue token` через `core-api`;
- polling `token grant status`;
- connect/disconnect через LiveKit SDK.

3. Контрактная дисциплина:
- frontend генерирует типы/клиент из backend OpenAPI;
- контракт хранится в `contracts/` frontend репо и синхронизируется с backend.

## 4. Beta Scope (Should Have)

- Улучшение UX/устойчивости voice-flow (retry/reconnect/error states).
- Безопасное хранение auth токенов для desktop-runtime.
- Расширение e2e/blackbox сценариев под пользовательские кейсы.
