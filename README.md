# Media API

Сервис принимает аудиофайл по `multipart/form-data`, валидирует его и публикует события в Kafka.

## Самодостаточность сервиса

Директория `Media_api` содержит всё, что нужно для сборки и запуска самого API:

- `Cargo.toml` и `Cargo.lock` - Rust-зависимости сервиса;
- `Dockerfile` - standalone-сборка образа API;
- `docker-compose.yml` - локальный интеграционный стенд;
- `.gitignore` и `.dockerignore` - исключают build artifacts, локальные env-файлы, editor files и временные файлы.

Код `Media_worker` и `Media_subtitle_worker` не нужен для сборки API. Связь с остальными сервисами идёт через Kafka topic `media` и общий volume с `TEMP_UPLOAD_DIR`, если downstream worker должен читать файл по `temp_path`.

## Что нужноs

- Docker Desktop / Docker Engine с `docker compose`
- свободные порты `8081` и `9092`

## Быстрый запуск в Docker

Запускать нужно из корня репозитория `Media_api`.

```bash
docker compose up -d --build kafka kafka-init media-api
```

Поднимаются только обязательные зависимости для `media-api`. `media-worker`, `media-subtitle-worker` и `rustfs` для проверки этого сервиса не нужны.

Проверить, что контейнеры поднялись:

```bash
docker compose ps
docker compose logs --tail=50 media-api
```

После старта сервис отдаёт OpenAPI JSON:

- `http://localhost:8081/api-docs/openapi.json`

Если нужен UI, этот JSON можно открыть во внешнем Swagger UI, Postman или Swagger Editor.

## Настройки

Для Docker Compose уже настроены значения по умолчанию:

- `KAFKA_BROKERS=kafka:9092`
- `JWT_SECRET=super-secret-key-change-me`
- `TEMP_UPLOAD_DIR=/tmp/media_uploads`
- `PORT=8081`

Если хотите поменять секрет для JWT, отредактируйте `JWT_SECRET` в `docker-compose.yml`.

Ограничения upload:

- максимальный размер файла: `50 MB`;
- поле формы: `audio`;
- поддерживаемые расширения: `mp3`, `wav`, `ogg`, `flac`, `opus`, `m4a`, `aac`;
- сервис проверяет не только расширение, но и magic bytes, чтобы отсеять переименованные не-audio файлы.

## Контракт Kafka

API публикует события в topic `media`. Все события имеют поле `event`.

Начало загрузки:

```json
{
  "event": "start_upload",
  "file_id": "uuid",
  "author_id": "user-id",
  "filename": "audio.mp3",
  "started_at": "2026-03-22T12:34:00Z"
}
```

Успешная загрузка:

```json
{
  "event": "uploaded",
  "file_id": "uuid",
  "author_id": "user-id",
  "size_bytes": 123456,
  "original_format": "audio/mpeg",
  "temp_path": "/tmp/media_uploads/<uuid>.mp3",
  "uploaded_at": "2026-03-22T12:34:10Z"
}
```

Ошибка:

```json
{
  "event": "error",
  "file_id": "uuid",
  "stage": "validation",
  "error_message": "...",
  "timestamp": "2026-03-22T12:34:10Z"
}
```

## Запуск на сервере

Dockerfile сервиса самодостаточен: для сборки нужны только файлы из директории `Media_api`. Образ не копирует код `Media_worker` или `Media_subtitle_worker`.

Для production обычно нужны:

- Kafka broker с заранее созданным topic `media`
- общий volume для временных upload-файлов, если `media-worker` читает `temp_path`
- сильный `JWT_SECRET`
- открытый порт API, по умолчанию `8081`

Минимальные переменные окружения:

```env
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092
JWT_SECRET=<strong-secret>
TEMP_UPLOAD_DIR=/tmp/media_uploads
PORT=8081
```

Сборка standalone-образа:

```bash
docker build -t media-api:latest .
```

Пример запуска:

```bash
docker run -d \
  --name media-api \
  --restart unless-stopped \
  -p 8081:8081 \
  -e KAFKA_BROKERS="kafka-1:9092,kafka-2:9092" \
  -e JWT_SECRET="<strong-secret>" \
  -e TEMP_UPLOAD_DIR="/tmp/media_uploads" \
  -e PORT="8081" \
  -v /srv/media/tmp:/tmp/media_uploads \
  media-api:latest
```

Если `media-worker` работает отдельно, он должен видеть тот же файл по тому же `temp_path`. Самый простой вариант - монтировать `/srv/media/tmp` в оба контейнера как `/tmp/media_uploads`.

`docker-compose.yml` в этой директории - интеграционный compose для всех сервисов. Он собирает соседние директории `../Media_worker` и `../Media_subtitle_worker`, поэтому для production его нужно адаптировать под реальные secrets, volumes, network и replication-factor.

## Проверка работоспособности

### 1. Проверка OpenAPI

```bash
curl -fsS http://localhost:8081/api-docs/openapi.json
```

### 2. Сгенерировать JWT

```bash
TOKEN=$(python3 -c 'import base64, json, hmac, hashlib, time; enc=lambda o: base64.urlsafe_b64encode(json.dumps(o,separators=(",",":")).encode()).rstrip(b"=").decode(); header=enc({"alg":"HS256","typ":"JWT"}); payload=enc({"sub":"test-user","exp":int(time.time())+3600,"iat":int(time.time())}); sig=base64.urlsafe_b64encode(hmac.new(b"super-secret-key-change-me", f"{header}.{payload}".encode(), hashlib.sha256).digest()).rstrip(b"=").decode(); print(f"{header}.{payload}.{sig}")')
```

### 3. Отправить тестовый файл

Поле формы должно называться `audio`.

```bash
curl -fsS -X POST http://localhost:8081/api/media/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "audio=@/absolute/path/to/file.mp3"
```

Ожидаемый ответ:

```json
{
  "success": true,
  "file_id": "...",
  "filename": "file.mp3",
  "size_bytes": 123456,
  "original_format": "audio/mpeg",
  "temp_path": "/tmp/media_uploads/....mp3",
  "message": "Файл загружен и отправлен на обработку"
}
```

## Остановка и очистка

```bash
docker compose down
```

С удалением volumes:

```bash
docker compose down -v
```

## Что проверено

Локально проверен сценарий:

- `cargo test --offline`
- `cargo build --release --offline`
- `docker compose up -d --build kafka kafka-init media-api`
- `curl http://localhost:8081/api-docs/openapi.json`
- `POST /api/media/upload` с валидным JWT и реальным mp3-файлом
