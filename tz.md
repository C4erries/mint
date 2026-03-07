# tz.md

## 1. Цель документа

Этот документ фиксирует более строгую постановку задачи для backend-части проекта **Mint**. Документ описывает:

- архитектурные решения, которые считаются зафиксированными;
- bounded contexts и границы ответственности;
- правила CQRS/DDD;
- перечень основных сервисов/бинарников;
- правила владения данными и интеграции между контекстами.

Документ ориентирован на дальнейшую реализацию в monorepo на Go.

## 2. Цель системы

Разработать backend для self-hosted desktop-приложения Mint — коммуникационной системы, объединяющей:

- Discord-подобную модель workspaces / channels / permissions / voice rooms;
- Telegram-подобную модель личного общения и удобного messaging flow;
- voice/video через **LiveKit**;
- умеренно-сервисную архитектуру без избыточного дробления.

## 3. Зафиксированные архитектурные решения

### 3.1. Репозиторий и бинарники

- Backend реализуется в **monorepo**.
- Каждый deployable unit имеет собственный `main.go` в `cmd/<service>/`.
- Один бинарник может поднимать HTTP/gRPC API, Kafka consumers, outbox relay и фоновые задачи, если это упрощает систему и не ломает границы контекстов.

### 3.2. DDD

Каждый bounded context строится по схеме:

- `domain` — бизнес-сущности, value objects, агрегаты, инварианты;
- `application` — use cases, command/query handlers, orchestration;
- `infrastructure` — transport/adapters с обязательным разделением на `in/` и `out/`, persistence, Kafka, Redis, LiveKit и прочая техническая реализация.

Требование: доменная модель остаётся чистой и не зависит от инфраструктурных деталей.

### 3.3. CQRS

Фиксируем следующие правила:

- **Write-side** работает через **Kafka**.
- Клиентский write-запрос не изменяет состояние напрямую.
- Desktop frontend работает по REST только с `core-api`.
- Синхронные межсервисные вызовы выполняются только по gRPC.
- API принимает команду, делает минимальную синхронную валидацию, публикует команду в Kafka и возвращает подтверждение принятия.
- Фактическое изменение состояния выполняется command consumer’ом в application layer соответствующего bounded context.
- **Read-side** обрабатывается напрямую query API/handlers без Kafka в запросном пути.
- Read model может быть денормализована и оптимизирована под UI.

Следствие: между write-model и read-model допускается eventual consistency.

### 3.4. Outbox

Outbox обязателен там, где изменение состояния должно надежно породить доменное или интеграционное событие.

Требования:
- запрещена схема “записали состояние и потом best effort отправили сообщение”;
- publish обязан быть надёжно восстанавливаем;
- consumers и projectors обязаны быть идемпотентны.

### 3.5. Основные технологии

- Backend: **Go**
- Messaging backbone: **Kafka**
- Query / durable storage: **ScyllaDB**
- Ephemeral/realtime state: **Redis**
- RTC/media transport: **LiveKit**
- Logging: **slog**
- Local environment: **Docker + docker-compose**

### 3.6. Transport matrix

- `frontend -> core-api`: REST.
- `core-api <-> backend services`: gRPC для синхронных межсервисных вызовов.
- Командные write-операции не выполняются через gRPC и публикуются в Kafka.

### 3.7. Политика зависимостей

- Используем только стабильные релизы библиотек (без alpha/beta/rc/pre-release).
- Версии зависимостей фиксируются явно в `go.mod`/`go.sum`.
- При обновлении зависимостей проверяем последние стабильные релизы по официальным источникам.

## 4. Системный контекст и модель верхнего уровня

Система состоит из bounded contexts, сгруппированных в несколько крупных сервисов.

Базовая группировка без чрезмерного дробления:

- `core-api`
- `messaging-api`
- `realtime-api`
- `rtc-api`
- `integration-api` (может быть добавлен позже, когда появится реальная потребность)

Допускается, что каждый сервис содержит:
- transport layer,
- command publication,
- command consumers своего контекста,
- outbox relay,
- query handlers,
- локальные projectors своего контекста.

То есть write-path идёт через Kafka, но инфраструктурно система не дробится на десяток отдельных процессов без необходимости.

## 5. Bounded contexts

### 5.1. Identity

**Назначение**  
Аутентификация, сессии, управление доступом уровня аккаунта.

**Отвечает за**
- регистрацию/создание auth account;
- логин;
- refresh/access tokens;
- device sessions;
- logout / revoke session;
- блокировки уровня аккаунта, если они появятся.

**Основные сущности**
- `Account`
- `Session`
- `RefreshToken`
- `AuthProviderLink` (если появятся внешние провайдеры)

**Команды**
- `RegisterAccount`
- `Login`
- `RefreshSession`
- `LogoutSession`
- `RevokeAllSessions`

**Запросы**
- `GetSession`
- `GetActiveSessions`
- `GetAccountAuthState`

**События**
- `AccountRegistered`
- `SessionStarted`
- `SessionRefreshed`
- `SessionRevoked`

**Замечание**  
Identity не владеет пользовательским профилем, аватаром, social graph и workspace membership.

---

### 5.2. Profile / Social

**Назначение**  
Публичная и пользовательская “персона” в системе.

**Отвечает за**
- профиль пользователя;
- display name;
- avatar metadata/reference;
- privacy settings;
- relationships / friends / blocked users;
- пользовательские настройки, связанные с социальным поведением.

**Основные сущности**
- `UserProfile`
- `Relationship`
- `BlockEntry`
- `PrivacySettings`

**Команды**
- `CreateProfile`
- `UpdateProfile`
- `SendFriendRequest`
- `AcceptFriendRequest`
- `RejectFriendRequest`
- `BlockUser`
- `UnblockUser`
- `UpdatePrivacySettings`

**Запросы**
- `GetProfile`
- `GetRelationship`
- `GetPrivacySettings`
- `ListFriends`
- `ListBlockedUsers`

**События**
- `ProfileCreated`
- `ProfileUpdated`
- `FriendRequestSent`
- `FriendRequestAccepted`
- `UserBlocked`
- `UserUnblocked`
- `PrivacySettingsUpdated`

---

### 5.3. Workspace

**Назначение**  
Discord-подобная модель пространств: workspaces, categories, channels, роли и права.

**Отвечает за**
- создание и настройку workspaces;
- membership;
- роли;
- permission overrides;
- категории;
- text/voice channel definitions;
- invites;
- moderation в рамках workspace.

**Основные сущности / агрегаты**
- `Workspace`
- `WorkspaceMember`
- `Role`
- `Channel`
- `Category`
- `Invite`

**Команды**
- `CreateWorkspace`
- `RenameWorkspace`
- `CreateChannel`
- `UpdateChannel`
- `ArchiveChannel`
- `InviteMember`
- `JoinWorkspace`
- `LeaveWorkspace`
- `KickMember`
- `BanMember`
- `CreateRole`
- `UpdateRole`
- `AssignRole`
- `RevokeRole`
- `UpdatePermissions`

**Запросы**
- `GetWorkspace`
- `ListWorkspacesForUser`
- `GetWorkspaceMembers`
- `GetChannel`
- `ListChannels`
- `GetWorkspaceRoles`
- `ResolvePermissionsForUser`

**События**
- `WorkspaceCreated`
- `WorkspaceRenamed`
- `ChannelCreated`
- `ChannelUpdated`
- `MemberInvited`
- `MemberJoinedWorkspace`
- `MemberLeftWorkspace`
- `MemberKicked`
- `RoleCreated`
- `RoleAssigned`
- `PermissionsUpdated`

**Замечание**  
Workspace определяет правила доступа к каналам и voice room’ам, но не хранит сами сообщения и не управляет media transport.

---

### 5.4. Messaging

**Назначение**  
Все durable-сценарии общения: channel messages, direct messages, group chats, reactions, read state.

**Отвечает за**
- создание conversations/DM;
- отправку сообщений;
- редактирование и удаление сообщений;
- реакции;
- pinned messages;
- history;
- read markers;
- message metadata и связи с attachments.

**Основные сущности / агрегаты**
- `Conversation`
- `Message`
- `MessageRevision`
- `Reaction`
- `ReadMarker`

**Команды**
- `CreateDirectConversation`
- `CreateGroupConversation`
- `SendMessage`
- `EditMessage`
- `DeleteMessage`
- `AddReaction`
- `RemoveReaction`
- `PinMessage`
- `UnpinMessage`
- `MarkConversationRead`

**Запросы**
- `GetConversation`
- `ListUserConversations`
- `ListChannelMessages`
- `ListDirectMessages`
- `GetMessage`
- `GetUnreadCounters`

**События**
- `DirectConversationCreated`
- `GroupConversationCreated`
- `MessageSent`
- `MessageEdited`
- `MessageDeleted`
- `ReactionAdded`
- `ReactionRemoved`
- `ConversationMarkedRead`

**Замечание**  
Messaging не владеет хранением бинарных объектов. Для файлов используется Media context.

---

### 5.5. Media

**Назначение**  
Управление файлами и вложениями.

**Отвечает за**
- upload sessions;
- file metadata;
- привязку медиаобъектов к сообщениям;
- валидацию допустимого контента/размеров;
- thumbnails / preview metadata (по мере необходимости).

**Основные сущности**
- `MediaObject`
- `UploadSession`
- `Attachment`

**Команды**
- `InitiateUpload`
- `CompleteUpload`
- `AttachMediaToMessage`
- `DeleteMediaObject`

**Запросы**
- `GetUploadSession`
- `GetMediaObject`
- `ListAttachmentsForMessage`

**События**
- `UploadInitiated`
- `UploadCompleted`
- `MediaAttachedToMessage`
- `MediaDeleted`

---

### 5.6. Realtime / Presence

**Назначение**  
Эфемерное состояние пользователей и realtime fanout для desktop-клиента.

**Отвечает за**
- online/offline/idle/dnd;
- active websocket connections;
- typing indicators;
- presence в каналах/экранах/комнатах на уровне UI;
- доставку realtime событий клиентам.

**Основные сущности**
- `PresenceState`
- `ClientConnection`
- `TypingSession`

**Команды**
- `RegisterConnection`
- `UnregisterConnection`
- `SetPresence`
- `StartTyping`
- `StopTyping`

**Запросы**
- `GetPresence`
- `ListOnlineUsersForWorkspace`
- `GetTypingState`

**События**
- `ConnectionRegistered`
- `ConnectionClosed`
- `PresenceChanged`
- `TypingStarted`
- `TypingStopped`

**Замечание**  
Realtime/Presence не является durable source of truth для сообщений, membership или workspace state.

---

### 5.7. RTC

**Назначение**  
Оркестрация голосовых и видеокомнат поверх LiveKit.

**Отвечает за**
- выдачу access/token на подключение к LiveKit;
- связь `workspace/channel -> rtc room`;
- lifecycle voice room;
- participant voice state;
- join/leave/mute/deafen/camera state в терминах приложения;
- трансляцию transport-событий LiveKit в доменные/realtime события.

**Основные сущности**
- `VoiceRoom`
- `VoiceParticipantSession`
- `VoiceChannelBinding`
- `MediaAccessGrant`

**Команды**
- `JoinVoiceChannel`
- `LeaveVoiceChannel`
- `MuteSelf`
- `UnmuteSelf`
- `EnableCamera`
- `DisableCamera`
- `IssueRtcToken`
- `TerminateVoiceSession`

**Запросы**
- `GetVoiceRoomState`
- `ListVoiceParticipants`
- `GetVoiceChannelBinding`

**События**
- `VoiceChannelJoined`
- `VoiceChannelLeft`
- `MicrophoneMuted`
- `MicrophoneUnmuted`
- `CameraEnabled`
- `CameraDisabled`
- `RtcTokenIssued`

**Замечание**  
LiveKit — транспортный/media слой. RTC context хранит бизнесовое и прикладное состояние, а не низкоуровневые WebRTC детали.

---

### 5.8. Notification

**Назначение**  
Нотификации и unread/mention-модель.

**Отвечает за**
- in-app notifications;
- unread counters;
- mention/alert projections;
- пользовательские настройки нотификаций.

**Основные сущности**
- `Notification`
- `NotificationPreference`
- `UnreadCounter`

**Команды**
- `UpdateNotificationPreferences`
- `AcknowledgeNotification`
- `MarkNotificationRead`

**Запросы**
- `ListNotifications`
- `GetUnreadCounters`
- `GetNotificationPreferences`

**События**
- `NotificationCreated`
- `NotificationAcknowledged`
- `NotificationMarkedRead`
- `NotificationPreferencesUpdated`

**Замечание**  
Часть нотификаций и unread может строиться проекциями на события Messaging/Workspace/RTC.

---

### 5.9. Integration

**Назначение**  
Интеграции, мосты, боты и внешние провайдеры.

**Отвечает за**
- внешние webhook-подписки;
- bot/integration registrations;
- Telegram bridge или иные chat-интеграции, когда они реально появятся;
- преобразование внешних событий в внутренние команды/события.

**Основные сущности**
- `Integration`
- `Bot`
- `ExternalBinding`
- `WebhookEndpoint`

**Команды**
- `RegisterIntegration`
- `EnableIntegration`
- `DisableIntegration`
- `ReceiveExternalMessage`
- `PublishExternalMessage`

**Запросы**
- `ListIntegrations`
- `GetIntegrationStatus`

**События**
- `IntegrationRegistered`
- `IntegrationEnabled`
- `ExternalMessageReceived`
- `ExternalMessagePublished`

**Замечание**  
Integration context не должен размывать ядро системы. Внешние провайдеры подключаются через отдельные адаптеры и события.

## 6. Разбиение на сервисы / бинарники

### 6.1. `core-api`

**Состав контекстов**
- Identity
- Profile / Social
- Workspace

**Назначение**
- внешний REST/BFF фасад для desktop frontend;
- базовые API аккаунта, профиля и workspace-модели;
- чтение соответствующих query models;
- синхронные межсервисные query/orchestration вызовы по gRPC без переноса доменной логики в `core-api`;
- публикация команд этих контекстов в Kafka;
- обработка команд и локальных projectors этих контекстов (допустимо в том же бинарнике на старте).

### 6.2. `messaging-api`

**Состав контекстов**
- Messaging
- Media
- Notification (или его базовая часть)

**Назначение**
- API сообщений, history, reactions, uploads, unread;
- публикация messaging/media команд;
- command consumers и проекции messaging/read models.

### 6.3. `realtime-api`

**Состав контекстов**
- Realtime / Presence

**Назначение**
- websocket gateway;
- online/offline/typing;
- fanout событий для клиента;
- доставка realtime изменений из других контекстов в UI-friendly поток.

### 6.4. `rtc-api`

**Состав контекстов**
- RTC

**Назначение**
- orchestration voice/video;
- выдача LiveKit credentials;
- управление voice participant state;
- приём и нормализация LiveKit callbacks/events;
- интеграция с Workspace permissions и Realtime fanout.

### 6.5. `integration-api` / `integration-worker` (не обязательно в MVP)

**Состав контекстов**
- Integration

**Назначение**
- внешние мосты, боты, webhook integrations;
- отдельный контур, чтобы внешний хаос не смешивался с ядром системы.

## 7. Правила владения данными

- Каждый bounded context владеет своими write-model и read-model таблицами/коллекциями/ключами.
- Нельзя читать чужое write-хранилище “напрямую из базы”.
- Интеграция между контекстами допускается:
  - через события,
  - через явные API,
  - через специально построенные query projections.
- Realtime/Presence хранит эфемерные данные в Redis и не заменяет durable state.
- LiveKit не считается владельцем бизнес-правил доступа; правила доступа принадлежат приложению.
- Webhook/callback внешних систем (например, LiveKit) считаются входящими (`in`) адаптерами целевого сервиса и не являются межсервисным API.

## 8. CQRS-потоки

### 8.1. Write flow

Общая схема:
1. Desktop frontend отправляет команду в REST API `core-api`.
2. `core-api` валидирует аутентификацию, базовый формат и минимум preconditions.
3. `core-api` публикует команду в Kafka.
4. Command consumer соответствующего контекста обрабатывает команду.
5. Handler изменяет write model.
6. В той же единице надежности фиксируется outbox запись, если после изменения состояния должно уйти событие.
7. Outbox relay публикует доменные/integration события.
8. Projectors обновляют read models.
9. Realtime получает событие и доставляет обновление клиентам.

### 8.2. Read flow

Общая схема:
1. Desktop frontend делает REST query-запрос в `core-api`.
2. `core-api` читает свою read model напрямую либо делает gRPC query в целевой сервис.
3. Query handler целевого контекста читает уже готовую read model / projection напрямую.
4. Ответ возвращается без Kafka в запросном пути.

## 9. Требования к моделям и контрактам

- Команды и query DTO разделены.
- Доменная модель не смешивается с транспортными и persistence-моделями.
- События версионируются при изменении контракта.
- Имена команд — в imperative форме.
- Имена событий — в past tense.
- Все message/event/command handlers должны быть идемпотентны.

## 10. Требования к тестированию и качеству

Обязательные требования:
- unit tests — табличные, через `t.Run`;
- использовать `t.Parallel()` там, где это безопасно;
- использовать `testify`;
- моки генерировать `mockery` через `mockery.yml`;
- ошибки в тестах не игнорировать;
- при найденной логической ошибке исправлять код, а не “подгонять” тест;
- при исправлении логической ошибки добавлять регрессионный тест.

## 11. Ограничения ближайшего этапа

На старте не требуется:
- собственный SFU/WebRTC сервер вместо LiveKit;
- чрезмерное дробление по одному сервису на сущность;
- публичный интернет-масштаб;
- полноценная мультирегиональность;
- сложная интеграционная платформа до появления реального запроса.

## 12. Практический результат, который должен получиться

В результате должна получиться система, где:

- backend остаётся чисто разделён на bounded contexts;
- write-путь проходит через Kafka;
- read-путь остаётся прямым и быстрым;
- события не теряются там, где они обязательны;
- доменная модель остаётся чистой;
- voice/video реализуются через LiveKit без попадания transport-detail в домен;
- набор сервисов достаточен для развития, но не раздроблен без необходимости.

