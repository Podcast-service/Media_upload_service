# Kafka Contract

`Media_upload_service` не читает Kafka-сообщения. После обработки HTTP upload-запроса
сервис публикует JSON-события в два топика. Kafka key во всех сообщениях равен
`object_id`.

| Topic | Назначение | События |
| --- | --- | --- |
| `media` | Generic-поток для `Media_worker` | `start_upload`, `uploaded`, `error` |
| `media.upload` | Backend-поток для `podcast_core` | `start_upload`, `uploaded`, `error` |

Допустимые типы медиа:

| HTTP upload | `media.type` | `media.upload.object_type` |
| --- | --- | --- |
| Аудиофайл подкаста | `podcast_file` | `podcast_file_url` |
| Аватар | `avatar` | `avatar` |
| Обложка подкаста | `podcast_cover` | `podcast_cover_url` |
| Обложка плейлиста | `playlists` | `playlist` |

## Topic `media`

Generic-поток предназначен для `Media_worker`. Поле `url` в событии `uploaded`
является S3 locator в формате `s3://<bucket>/<object_key>`.

### `start_upload`

```json
{
  "event": "start_upload",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "started_at": "2026-04-07T12:00:00Z"
}
```

### `uploaded`

```json
{
  "event": "uploaded",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "url": "s3://4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_file/11111111-1111-1111-1111-111111111111/22222222-2222-4222-8222-222222222222.mp3",
  "size": 123456,
  "content_type": "audio/mpeg",
  "need_subtitle": true,
  "uploaded_at": "2026-04-07T12:00:10Z"
}
```

`Media_upload_service` всегда публикует `need_subtitle=true`. Получатель может
использовать это поле, чтобы решить, нужно ли запрашивать генерацию субтитров.

### `error`

```json
{
  "event": "error",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "error_message": "Unsupported media type",
  "timestamp": "2026-04-07T12:00:10Z"
}
```

## Topic `media.upload`

Backend-поток предназначен для `podcast_core`. URL в этих сообщениях доступен
по HTTP и строится из `S3_ENDPOINT_URL`.

### `start_upload`

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "start_upload",
  "timestamp": "2026-04-07T12:00:00Z"
}
```

### `uploaded` для аудиофайла

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "uploaded",
  "audio_url_file": "https://s3.twcstorage.ru/4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_file/11111111-1111-1111-1111-111111111111/22222222-2222-4222-8222-222222222222.mp3",
  "timestamp": "2026-04-07T12:00:10Z"
}
```

### `uploaded` для изображения

```json
{
  "object_type": "podcast_cover_url",
  "object_id": "22222222-2222-2222-2222-222222222222",
  "event": "uploaded",
  "image_url": "https://s3.twcstorage.ru/4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_cover/22222222-2222-2222-2222-222222222222/33333333-3333-4333-8333-333333333333.webp",
  "timestamp": "2026-04-07T12:00:10Z"
}
```

### `error`

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "error",
  "error": "Unsupported media type",
  "timestamp": "2026-04-07T12:00:10Z"
}
```

## Notes

- Все timestamp-поля сериализуются как RFC 3339 UTC.
- Поля без значения не сериализуются в `media.upload`.
- Для `media.upload.uploaded` аудиофайл содержит `audio_url_file`, а изображение
  содержит `image_url`.
