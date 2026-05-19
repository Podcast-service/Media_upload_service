const ALLOWED_EXTENSIONS: &[&str] = &["mp3", "wav", "ogg", "flac", "opus", "m4a", "aac"];

pub const MAX_FILE_SIZE: usize = 50 * 1024 * 1024;

pub fn validate_extension(filename: &str) -> Result<String, String> {
    let ext = filename
        .rsplit('.')
        .next()
        .map(|e| e.to_lowercase())
        .ok_or_else(|| "Файл не имеет расширения".to_string())?;

    if ext.is_empty() {
        return Err("Файл не имеет расширения".to_string());
    }

    if ALLOWED_EXTENSIONS.contains(&ext.as_str()) {
        Ok(ext)
    } else {
        Err(format!(
            "Недопустимое расширение '.{}'. Разрешены: {}",
            ext,
            ALLOWED_EXTENSIONS.join(", ")
        ))
    }
}

pub fn validate_magic_bytes(data: &[u8]) -> Result<&'static str, String> {
    if data.len() < 12 {
        return Err("Файл слишком мал для определения формата".to_string());
    }

    // WAV: RIFF....WAVE
    if &data[0..4] == b"RIFF" && &data[8..12] == b"WAVE" {
        return Ok("wav");
    }

    // FLAC
    if &data[0..4] == b"fLaC" {
        return Ok("flac");
    }

    // OGG
    if &data[0..4] == b"OggS" {
        return Ok("ogg");
    }

    // MP3 with ID3 tag
    if data.len() >= 3 && &data[0..3] == b"ID3" {
        return Ok("mp3");
    }

    // MP3 sync word / AAC ADTS
    if data[0] == 0xFF && (data[1] & 0xE0) == 0xE0 {
        let layer_bits = data[1] & 0x06;
        if layer_bits == 0x00 && (data[1] & 0xF0) == 0xF0 {
            return Ok("aac");
        } else {
            return Ok("mp3");
        }
    }

    // M4A / AAC in MP4 container
    if data.len() >= 8 && &data[4..8] == b"ftyp" {
        return Ok("m4a");
    }

    Err("Не удалось определить формат по заголовку файла. Файл не является аудио.".to_string())
}

pub fn check_extension_magic_compatibility(extension: &str, detected: &str) -> Result<(), String> {
    let compatible = match (extension, detected) {
        (a, b) if a == b => true,
        ("opus", "ogg") => true,
        ("aac", "m4a") => true,
        ("m4a", "aac") => true,
        _ => false,
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
        _ => "application/octet-stream",
    }
}
