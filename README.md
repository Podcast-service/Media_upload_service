# Media API

Сервис принимает `multipart/form-data`, проверяет аудио или изображение по расширению и magic bytes, загружает файл в S3-compatible storage и публикует generic-события в Kafka topic `media`.
.
## Интеграционный запуск с S3

`docker-compose.yml` в этой директории прокидывает в downstream worker'ы внешний S3-compatible storage:

```env
S3_ENDPOINT_URL=https://s3.twcstorage.ru
S3_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
AUDIO_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
HLS_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
SUBTITLE_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
S3_REGION=ru-1
S3_ACCESS_KEY_ID=<secret>
S3_SECRET_ACCESS_KEY=<secret>
```

Секреты кладите в локальный `.env` рядом с `docker-compose.yml`; `.env` игнорируется git. В репозитории оставлен только безопасный `.env.example`.

## Быстрый запуск в Docker

Запускать нужно из корня `Media_upload_service`.

```bash
docker compose up -d --build kafka kafka-init media-api
```

Проверить, что контейнеры поднялись:

```bash
docker compose ps
docker compose logs --tail=50 media-api
```

После старта сервис отдаёт OpenAPI JSON:

- `http://localhost:8081/api-docs/openapi.json`

## Настройки

Для Docker Compose уже настроены значения по умолчанию:

- `KAFKA_BROKERS=kafka:9092`
- `JWT_SECRET=super-secret-key-change-me`
- `PORT=8081`
- `S3_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3`
- `AUDIO_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3`

Минимальные production-переменные:

```env
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092
JWT_SECRET=<strong-secret>
PORT=8081
S3_ENDPOINT_URL=https://s3.twcstorage.ru
S3_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
AUDIO_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
S3_REGION=ru-1
S3_ACCESS_KEY_ID=<secret>
S3_SECRET_ACCESS_KEY=<secret>
```

## HTTP API

Все upload-роуты требуют `Authorization: Bearer <JWT>`. Поля `id_podcast`, `id_playlist` и JWT `user_id` должны быть UUID. Для обратной совместимости также принимается JWT `sub`.

| Route | Multipart fields | Kafka `type` | `object_id` |
| --- | --- | --- | --- |
| `POST /api/media/upload_audio` | `id_podcast`, `audio` | `podcast_file` | `id_podcast` |
| `POST /api/media/upload_cover_profile` | `image` | `avatar` | JWT `user_id` |
| `POST /api/media/upload_cover_podcast` | `id_podcast`, `image` | `podcast_cover` | `id_podcast` |
| `POST /api/media/upload_cover_playlist` | `id_playlist`, `image` | `playlists` | `id_playlist` |

Ограничения upload:

- максимальный размер файла: `50 MB`;
- аудио: `mp3`, `wav`, `ogg`, `flac`, `opus`, `m4a`, `aac`;
- изображения: `jpg`, `jpeg`, `png`, `webp`;
- сервис проверяет расширение и magic bytes, чтобы отсеять переименованные файлы.

Успешный ответ:

```json
{
  "success": true,
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "url": "s3://4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_file/11111111-1111-1111-1111-111111111111/<upload-id>.mp3",
  "size": 123456,
  "content_type": "audio/mpeg",
  "filename": "episode.mp3",
  "message": "Аудиофайл сохранен в S3 и отправлен на обработку"
}
```

## Kafka Contract

API публикует события в topic `media`. Все события имеют поля `event`, `type` и `object_id`.
Эти сообщения сохраняются для коммуникации с `Media_worker`.

Начало загрузки:

```json
{
  "event": "start_upload",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "started_at": "2026-03-22T12:34:00Z"
}
```

Успешная загрузка:

```json
{
  "event": "uploaded",
  "type": "podcast_cover",
  "object_id": "22222222-2222-4222-8222-222222222222",
  "url": "s3://4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_cover/22222222-2222-4222-8222-222222222222/<upload-id>.webp",
  "size": 123456,
  "content_type": "image/webp",
  "uploaded_at": "2026-03-22T12:34:10Z"
}
```

Ошибка:

```json
{
  "event": "error",
  "type": "avatar",
  "object_id": "33333333-3333-4333-8333-333333333333",
  "error_message": "Unsupported media type",
  "timestamp": "2026-03-22T12:34:10Z"
}
```

`Media_worker` обрабатывает только `event=uploaded` + `type=podcast_file`. Cover/avatar-события остаются в topic `media` для backend-потребителей и игнорируются worker'ом.

Для backend API дополнительно публикует совместимые события в topic `media.upload`.
Для аудиофайла используются `object_type=podcast_file_url` и `audio_url_file`:

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "uploaded",
  "audio_url_file": "s3://bucket/media/uploads/podcast_file/11111111-1111-1111-1111-111111111111/source.mp3",
  "timestamp": "2026-05-31T00:00:00Z"
}
```

Для обложек и аватаров вместо `audio_url_file` публикуется `image_url`.

## Проверка работоспособности

### 1. Проверка OpenAPI

```bash
curl -fsS http://localhost:8081/api-docs/openapi.json
```

### 2. Сгенерировать JWT

```bash
TOKEN=$(python3 -c 'import base64, json, hmac, hashlib, time; enc=lambda o: base64.urlsafe_b64encode(json.dumps(o,separators=(",",":")).encode()).rstrip(b"=").decode(); header=enc({"alg":"HS256","typ":"JWT"}); payload=enc({"user_id":"11111111-1111-4111-8111-111111111111","exp":int(time.time())+3600,"iat":int(time.time())}); sig=base64.urlsafe_b64encode(hmac.new(b"super-secret-key-change-me", f"{header}.{payload}".encode(), hashlib.sha256).digest()).rstrip(b"=").decode(); print(f"{header}.{payload}.{sig}")')
```

### 3. Отправить аудио

```bash
curl -fsS -X POST http://localhost:8081/api/media/upload_audio \
  -H "Authorization: Bearer $TOKEN" \
  -F "id_podcast=22222222-2222-4222-8222-222222222222" \
  -F "audio=@/absolute/path/to/file.mp3"
```

### 4. Отправить изображение профиля

```bash
curl -fsS -X POST http://localhost:8081/api/media/upload_cover_profile \
  -H "Authorization: Bearer $TOKEN" \
  -F "image=@/absolute/path/to/avatar.png"
```

## Остановка и очистка

```bash
docker compose down
```

С удалением volumes:

```bash
docker compose down -v
```

## Что проверять после изменений

- `cargo test --offline`
- `cargo build --release --offline`
- `docker compose up -d --build kafka kafka-init media-api`
- `curl http://localhost:8081/api-docs/openapi.json`
- `POST /api/media/upload_audio` с валидным JWT и реальным аудиофайлом
- `POST /api/media/upload_cover_profile` с валидным JWT и реальным изображением
