use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::Value;

pub fn cmd_pretty(content: &str, indent: usize) -> Result<()> {
    let val: Value = serde_json::from_str(content).context("Invalid JSON")?;

    let indent_bytes = " ".repeat(indent).into_bytes();
    let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
    let mut buf = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
    use serde::Serialize;
    val.serialize(&mut ser)?;

    println!("{}", String::from_utf8_lossy(&buf));
    Ok(())
}

pub fn cmd_minify(content: &str) -> Result<()> {
    let val: Value = serde_json::from_str(content).context("Invalid JSON")?;

    let out = serde_json::to_string(&val)?;

    let savings = if !content.is_empty() {
        let ratio = (1.0 - out.len() as f64 / content.len() as f64) * 100.0;
        format!(" ({:.0}% smaller)", ratio)
    } else {
        String::new()
    };

    println!("{}", out);
    eprintln!(
        "\n  {} {} → {} bytes{}",
        "✓".green().bold(),
        format_size(content.len()),
        format_size(out.len()),
        savings.dimmed()
    );
    Ok(())
}

pub fn cmd_validate(content: &str, file_label: &str) -> Result<()> {
    match serde_json::from_str::<Value>(content) {
        Ok(val) => {
            let type_desc = match &val {
                Value::Array(arr) => format!("Array ({} elements)", arr.len()),
                Value::Object(map) => format!("Object ({} keys)", map.len()),
                Value::String(_) => "String".to_string(),
                Value::Number(_) => "Number".to_string(),
                Value::Bool(_) => "Boolean".to_string(),
                Value::Null => "Null".to_string(),
            };

            println!(
                "  {} {} is valid JSON",
                "✓".green().bold(),
                file_label.bold()
            );
            println!("  {}      {}", "Type".bold(), type_desc);
            println!("  {}      {}", "Size".bold(), format_size(content.len()));

            if let Value::Object(map) = &val {
                let keys: Vec<&String> = map.keys().take(8).collect();
                let extra = if map.len() > 8 {
                    format!(", … +{}", map.len() - 8)
                } else {
                    String::new()
                };
                println!(
                    "  {}      {}{}",
                    "Keys".bold(),
                    keys.iter()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    extra.dimmed()
                );
            }
            if let Value::Array(arr) = &val {
                let depth = measure_depth(&val);
                println!("  {}     {}", "Depth".bold(), depth);
                if let Some(Value::Object(first)) = arr.first() {
                    let keys: Vec<&String> = first.keys().take(8).collect();
                    println!(
                        "  {}    {}",
                        "Fields".bold(),
                        keys.iter()
                            .map(|k| k.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }
        Err(e) => {
            println!(
                "  {} {} is NOT valid JSON",
                "✗".red().bold(),
                file_label.bold()
            );
            println!(
                "  {}     line {}, column {}",
                "Error".bold(),
                e.line(),
                e.column()
            );
            println!("  {}   {}", "Detail".bold(), e.to_string().red());
        }
    }
    Ok(())
}

pub fn cmd_flatten(content: &str, separator: &str) -> Result<()> {
    let val: Value = serde_json::from_str(content).context("Invalid JSON")?;

    match &val {
        Value::Array(arr) => {
            let mut all_flat: Vec<Value> = Vec::new();
            for item in arr {
                let mut flat = serde_json::Map::new();
                flatten_value(item, "", separator, &mut flat);
                all_flat.push(Value::Object(flat));
            }
            println!("{}", serde_json::to_string_pretty(&all_flat)?);
        }
        Value::Object(_) => {
            let mut flat = serde_json::Map::new();
            flatten_value(&val, "", separator, &mut flat);
            println!("{}", serde_json::to_string_pretty(&Value::Object(flat))?);
        }
        _ => println!("{}", serde_json::to_string_pretty(&val)?),
    }
    Ok(())
}

fn flatten_value(val: &Value, prefix: &str, sep: &str, out: &mut serde_json::Map<String, Value>) {
    match val {
        Value::Object(map) => {
            for (key, v) in map {
                let new_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}{}{}", prefix, sep, key)
                };
                flatten_value(v, &new_key, sep, out);
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_key = format!(
                    "{}{}[{}]",
                    prefix,
                    if prefix.is_empty() { "" } else { sep },
                    i
                );
                flatten_value(v, &new_key, sep, out);
            }
        }
        _ => {
            out.insert(prefix.to_string(), val.clone());
        }
    }
}

fn measure_depth(val: &Value) -> usize {
    match val {
        Value::Array(arr) => 1 + arr.iter().map(measure_depth).max().unwrap_or(0),
        Value::Object(map) => 1 + map.values().map(measure_depth).max().unwrap_or(0),
        _ => 0,
    }
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten_simple() {
        let val: Value = serde_json::json!({"a": {"b": 1, "c": 2}});
        let mut flat = serde_json::Map::new();
        flatten_value(&val, "", ".", &mut flat);
        assert_eq!(flat.get("a.b"), Some(&Value::Number(1.into())));
        assert_eq!(flat.get("a.c"), Some(&Value::Number(2.into())));
    }

    #[test]
    fn test_flatten_array() {
        let val: Value = serde_json::json!({"tags": ["a", "b"]});
        let mut flat = serde_json::Map::new();
        flatten_value(&val, "", ".", &mut flat);
        assert_eq!(flat.get("tags.[0]"), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn test_measure_depth() {
        let val: Value = serde_json::json!({"a": {"b": {"c": 1}}});
        assert_eq!(measure_depth(&val), 3);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500 B");
        assert_eq!(format_size(2048), "2.0 KB");
        assert_eq!(format_size(2_097_152), "2.0 MB");
    }
}
