#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Audio,
    Image,
}

const AUDIO_EXTENSIONS: &[&str] = &["mp3", "wav", "ogg", "flac", "opus", "m4a", "aac"];
const IMAGE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "webp"];

pub const MAX_FILE_SIZE: usize = 50 * 1024 * 1024;

pub fn validate_extension(filename: &str, kind: FileKind) -> Result<String, String> {
    let ext = filename
        .rsplit_once('.')
        .map(|(_, e)| e.to_lowercase())
        .ok_or_else(|| "Файл не имеет расширения".to_string())?;

    if ext.is_empty() {
        return Err("Файл не имеет расширения".to_string());
    }

    let allowed = allowed_extensions(kind);
    if allowed.contains(&ext.as_str()) {
        Ok(ext)
    } else {
        Err(format!(
            "Недопустимое расширение '.{}'. Разрешены: {}",
            ext,
            allowed.join(", ")
        ))
    }
}

pub fn validate_magic_bytes(data: &[u8], kind: FileKind) -> Result<&'static str, String> {
    match kind {
        FileKind::Audio => validate_audio_magic_bytes(data),
        FileKind::Image => validate_image_magic_bytes(data),
    }
}

pub fn check_extension_magic_compatibility(
    extension: &str,
    detected: &str,
    kind: FileKind,
) -> Result<(), String> {
    let compatible = match kind {
        FileKind::Audio => match (extension, detected) {
            (a, b) if a == b => true,
            ("opus", "ogg") => true,
            ("aac", "m4a") => true,
            ("m4a", "aac") => true,
            _ => false,
        },
        FileKind::Image => match (extension, detected) {
            ("jpg" | "jpeg", "jpeg") => true,
            (a, b) if a == b => true,
            _ => false,
        },
    };

    if !compatible {
        return Err(format!(
            "Расширение '.{}' не соответствует реальному формату '{}'. Файл переименован?",
            extension, detected
        ));
    }

    Ok(())
}

pub fn mime_from_extension(ext: &str) -> &'static str {
    match ext {
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" | "opus" => "audio/ogg",
        "flac" => "audio/flac",
        "m4a" | "aac" => "audio/mp4",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

fn allowed_extensions(kind: FileKind) -> &'static [&'static str] {
    match kind {
        FileKind::Audio => AUDIO_EXTENSIONS,
        FileKind::Image => IMAGE_EXTENSIONS,
    }
}

fn validate_audio_magic_bytes(data: &[u8]) -> Result<&'static str, String> {
    if data.len() < 12 {
        return Err("Файл слишком мал для определения формата".to_string());
    }

    if &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        return Ok("wav");
    }

    if &data[0..4] == b"fLaC" {
        return Ok("flac");
    }

    if &data[0..4] == b"OggS" {
        return Ok("ogg");
    }

    if &data[0..3] == b"ID3" {
        return Ok("mp3");
    }

    if data[0] == 0xFF && (data[1] & 0xE0) == 0xE0 {
        let layer_bits = data[1] & 0x06;
        if layer_bits == 0x00 && (data[1] & 0xF0) == 0xF0 {
            return Ok("aac");
        } else {
            return Ok("mp3");
        }
    }

    if &data[4..8] == b"ftyp" {
        return Ok("m4a");
    }

    Err("Не удалось определить формат по заголовку файла. Файл не является аудио.".to_string())
}

fn validate_image_magic_bytes(data: &[u8]) -> Result<&'static str, String> {
    if data.len() < 12 {
        return Err("Файл слишком мал для определения формата".to_string());
    }

    if data.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Ok("jpeg");
    }

    if &data[0..8] == b"\x89PNG\r\n\x1A\n" {
        return Ok("png");
    }

    if &data[0..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        return Ok("webp");
    }

    Err(
        "Не удалось определить формат по заголовку файла. Файл не является изображением."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_audio_extension_case_insensitive() {
        assert_eq!(
            validate_extension("episode.MP3", FileKind::Audio).unwrap(),
            "mp3"
        );
    }

    #[test]
    fn rejects_audio_extension_for_image_kind() {
        assert!(validate_extension("cover.mp3", FileKind::Image).is_err());
    }

    #[test]
    fn detects_audio_magic_bytes() {
        assert_eq!(
            validate_magic_bytes(b"ID3\x04\x00\x00\x00\x00\x00\x00\x00\x00", FileKind::Audio)
                .unwrap(),
            "mp3"
        );
        assert_eq!(
            validate_magic_bytes(b"RIFF\x00\x00\x00\x00WAVEfmt ", FileKind::Audio).unwrap(),
            "wav"
        );
    }

    #[test]
    fn detects_image_magic_bytes() {
        assert_eq!(
            validate_magic_bytes(b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01", FileKind::Image).unwrap(),
            "jpeg"
        );
        assert_eq!(
            validate_magic_bytes(b"\x89PNG\r\n\x1A\n\x00\x00\x00\x0D", FileKind::Image).unwrap(),
            "png"
        );
        assert_eq!(
            validate_magic_bytes(b"RIFF\x00\x00\x00\x00WEBPVP8 ", FileKind::Image).unwrap(),
            "webp"
        );
    }

    #[test]
    fn rejects_renamed_image() {
        let err = check_extension_magic_compatibility("jpg", "png", FileKind::Image).unwrap_err();
        assert!(err.contains("не соответствует"));
    }

    #[test]
    fn rejects_unsupported_image_extension() {
        let err = validate_extension("avatar.gif", FileKind::Image).unwrap_err();
        assert!(err.contains("Недопустимое расширение"));
    }

    #[test]
    fn rejects_non_image_magic_bytes() {
        let err = validate_magic_bytes(b"not an image!", FileKind::Image).unwrap_err();
        assert!(err.contains("не является изображением"));
    }
}
