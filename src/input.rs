use anyhow::{Context, Result};
use std::io::{self, Read};
use std::path::PathBuf;
use tempfile::NamedTempFile;

pub enum JsonSource {
    File(String),
    Stdin(NamedTempFile),
}

impl JsonSource {
    pub fn path(&self) -> &str {
        match self {
            JsonSource::File(p) => p,
            JsonSource::Stdin(tmp) => tmp.path().to_str().unwrap_or("/tmp/stdin.json"),
        }
    }

    pub fn label(&self) -> &str {
        match self {
            JsonSource::File(p) => p,
            JsonSource::Stdin(_) => "<stdin>",
        }
    }
}

pub fn resolve_input(file_arg: &str) -> Result<JsonSource> {
    if file_arg == "-" {
        return read_stdin();
    }

    let path = PathBuf::from(file_arg);
    if path.exists() {
        return Ok(JsonSource::File(file_arg.to_string()));
    }

    anyhow::bail!("File not found: {}", file_arg)
}

fn read_stdin() -> Result<JsonSource> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .context("Failed to read from stdin")?;

    if buf.trim().is_empty() {
        anyhow::bail!("Empty input from stdin");
    }

    let mut tmp = NamedTempFile::new().context("Failed to create temporary file for stdin data")?;
    std::io::Write::write_all(&mut tmp, buf.as_bytes())
        .context("Failed to write stdin to temp file")?;

    Ok(JsonSource::Stdin(tmp))
}

pub fn read_content(file_arg: &str) -> Result<String> {
    if file_arg == "-" {
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .context("Failed to read from stdin")?;
        return Ok(buf);
    }
    std::fs::read_to_string(file_arg).with_context(|| format!("Failed to read file: {}", file_arg))
}
