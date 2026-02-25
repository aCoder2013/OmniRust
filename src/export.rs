use crate::engine::QueryResult;
use anyhow::{Context, Result};
use std::io::Write;

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Table,
    Csv,
    Json,
    Jsonl,
    Markdown,
}

impl OutputFormat {
    pub fn from_str_or_path(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "csv" => Ok(Self::Csv),
            "json" => Ok(Self::Json),
            "jsonl" | "ndjson" => Ok(Self::Jsonl),
            "md" | "markdown" => Ok(Self::Markdown),
            other => {
                if other.ends_with(".csv") {
                    Ok(Self::Csv)
                } else if other.ends_with(".json") {
                    Ok(Self::Json)
                } else if other.ends_with(".jsonl") || other.ends_with(".ndjson") {
                    Ok(Self::Jsonl)
                } else if other.ends_with(".md") {
                    Ok(Self::Markdown)
                } else {
                    anyhow::bail!(
                        "Unknown format '{}'. Use: csv, json, jsonl, md, or table.",
                        other
                    )
                }
            }
        }
    }
}

pub fn export_result(
    result: &QueryResult,
    format: &OutputFormat,
    path: Option<&str>,
) -> Result<()> {
    let output = match format {
        OutputFormat::Table => return Ok(()),
        OutputFormat::Csv => render_csv(result),
        OutputFormat::Json => render_json(result),
        OutputFormat::Jsonl => render_jsonl(result),
        OutputFormat::Markdown => render_markdown(result),
    };

    match path {
        Some(p) => {
            let mut f =
                std::fs::File::create(p).with_context(|| format!("Cannot write to {}", p))?;
            f.write_all(output.as_bytes())?;
            eprintln!("  ✓ Exported {} rows to {}", result.rows.len(), p);
        }
        None => print!("{}", output),
    }
    Ok(())
}

fn render_csv(result: &QueryResult) -> String {
    let mut out = String::new();
    let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    out.push_str(&csv_row(&headers));

    for row in &result.rows {
        let cells: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        out.push_str(&csv_row(&cells));
    }
    out
}

fn csv_row(fields: &[&str]) -> String {
    let escaped: Vec<String> = fields
        .iter()
        .map(|f| {
            if f.contains(',') || f.contains('"') || f.contains('\n') {
                format!("\"{}\"", f.replace('"', "\"\""))
            } else {
                f.to_string()
            }
        })
        .collect();
    format!("{}\n", escaped.join(","))
}

fn render_json(result: &QueryResult) -> String {
    let rows: Vec<serde_json::Value> = result
        .rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, col) in result.columns.iter().enumerate() {
                let val = row.get(i).map(|s| s.as_str()).unwrap_or("null");
                map.insert(col.name.clone(), parse_json_value(val));
            }
            serde_json::Value::Object(map)
        })
        .collect();

    serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string()) + "\n"
}

fn render_jsonl(result: &QueryResult) -> String {
    let mut out = String::new();
    for row in &result.rows {
        let mut map = serde_json::Map::new();
        for (i, col) in result.columns.iter().enumerate() {
            let val = row.get(i).map(|s| s.as_str()).unwrap_or("null");
            map.insert(col.name.clone(), parse_json_value(val));
        }
        let line = serde_json::to_string(&serde_json::Value::Object(map)).unwrap_or_default();
        out.push_str(&line);
        out.push('\n');
    }
    out
}

fn render_markdown(result: &QueryResult) -> String {
    let mut out = String::new();
    let headers: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();

    out.push_str("| ");
    out.push_str(&headers.join(" | "));
    out.push_str(" |\n");

    out.push_str("| ");
    out.push_str(
        &headers
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | "),
    );
    out.push_str(" |\n");

    for row in &result.rows {
        out.push_str("| ");
        out.push_str(&row.join(" | "));
        out.push_str(" |\n");
    }
    out
}

fn parse_json_value(s: &str) -> serde_json::Value {
    if s == "NULL" || s == "null" {
        return serde_json::Value::Null;
    }
    if let Ok(v) = s.parse::<i64>() {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(v) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return serde_json::Value::Number(n);
        }
    }
    if s == "true" {
        return serde_json::Value::Bool(true);
    }
    if s == "false" {
        return serde_json::Value::Bool(false);
    }
    serde_json::Value::String(s.to_string())
}
