use crate::engine::{ColumnInfo, QueryResult};
use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, ContentArrangement, Table};
use serde_json::Value;

pub fn render_table(result: &QueryResult) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);

    let headers: Vec<Cell> = result.columns.iter().map(|c| Cell::new(&c.name)).collect();
    table.set_header(headers);

    for row in &result.rows {
        let cells: Vec<Cell> = row.iter().map(Cell::new).collect();
        table.add_row(cells);
    }

    println!("{}", table);

    if let Some(total) = result.total_count {
        println!(
            "  {} rows total, showing {}",
            total.to_string().bold(),
            result.rows.len()
        );
    } else {
        println!("  {} rows", result.rows.len().to_string().bold());
    }
}

pub fn render_schema(columns: &[ColumnInfo]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec![Cell::new("#"), Cell::new("Column"), Cell::new("Type")]);

    for (i, col) in columns.iter().enumerate() {
        table.add_row(vec![
            Cell::new(i + 1),
            Cell::new(&col.name),
            Cell::new(&col.dtype),
        ]);
    }

    println!("{}", table);
    println!("  {} columns", columns.len().to_string().bold());
}

pub fn render_bar_chart(data: &[(String, i64)], column: &str) {
    if data.is_empty() {
        println!("{}", "  No data to display.".yellow());
        return;
    }

    println!(
        "\n  {} — value counts for '{}'",
        "Bar Chart".bold().cyan(),
        column.bold()
    );
    println!();

    let max_count = data.iter().map(|(_, c)| *c).max().unwrap_or(1);
    let max_label_len = data.iter().map(|(l, _)| l.len()).max().unwrap_or(0).min(24);
    let bar_width: usize = 40;

    for (label, count) in data {
        let truncated = if label.len() > 24 {
            format!("{}…", &label[..23])
        } else {
            label.clone()
        };

        let bar_len = if max_count > 0 {
            (*count as f64 / max_count as f64 * bar_width as f64) as usize
        } else {
            0
        }
        .max(1);

        let bar: String = "█".repeat(bar_len);
        println!(
            "  {:>width$} │ {} {}",
            truncated,
            bar.green(),
            count.to_string().dimmed(),
            width = max_label_len
        );
    }
    println!();
}

pub fn render_histogram(data: &[(f64, f64, i64)], column: &str) {
    if data.is_empty() {
        println!("{}", "  No data to display.".yellow());
        return;
    }

    println!(
        "\n  {} — distribution of '{}'",
        "Histogram".bold().cyan(),
        column.bold()
    );
    println!();

    let max_count = data.iter().map(|(_, _, c)| *c).max().unwrap_or(1);
    let bar_width: usize = 40;

    let format_num = |n: f64| -> String {
        if n.abs() >= 1_000_000.0 {
            format!("{:.1}M", n / 1_000_000.0)
        } else if n.abs() >= 1_000.0 {
            format!("{:.1}K", n / 1_000.0)
        } else if n.fract() == 0.0 {
            format!("{:.0}", n)
        } else {
            format!("{:.2}", n)
        }
    };

    let max_label_len = data
        .iter()
        .map(|(s, e, _)| format!("[{}, {})", format_num(*s), format_num(*e)).len())
        .max()
        .unwrap_or(0);

    for (start, end, count) in data {
        let label = format!("[{}, {})", format_num(*start), format_num(*end));

        let bar_len = if max_count > 0 {
            (*count as f64 / max_count as f64 * bar_width as f64) as usize
        } else {
            0
        }
        .max(if *count > 0 { 1 } else { 0 });

        let bar: String = "█".repeat(bar_len);
        println!(
            "  {:>width$} │ {} {}",
            label,
            bar.blue(),
            count.to_string().dimmed(),
            width = max_label_len
        );
    }
    println!();
}

pub fn render_jsonpath_results(results: &[Value], expr: &str) {
    if results.is_empty() {
        println!("  {}", "No results matched.".yellow());
        println!();
        println!(
            "  {} Run {} to see available syntax.",
            "Tip:".bold(),
            "omnirust json path --syntax".cyan()
        );
        return;
    }

    println!(
        "  {} {} matched {} result(s)\n",
        "✓".green().bold(),
        expr.cyan(),
        results.len().to_string().bold()
    );

    let all_objects = results.iter().all(Value::is_object);
    if all_objects && results.len() > 1 {
        render_objects_as_table(results);
    } else if results.len() == 1 {
        render_single_value(&results[0]);
    } else {
        render_value_list(results);
    }
}

fn render_objects_as_table(values: &[Value]) {
    let mut all_keys: Vec<String> = Vec::new();
    for val in values {
        if let Value::Object(map) = val {
            for key in map.keys() {
                if !all_keys.contains(key) {
                    all_keys.push(key.clone());
                }
            }
        }
    }

    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);

    let headers: Vec<Cell> = all_keys.iter().map(Cell::new).collect();
    table.set_header(headers);

    for val in values {
        if let Value::Object(map) = val {
            let cells: Vec<Cell> = all_keys
                .iter()
                .map(|k| {
                    let v = map.get(k).unwrap_or(&Value::Null);
                    Cell::new(format_value_compact(v))
                })
                .collect();
            table.add_row(cells);
        }
    }

    println!("{}", table);
    println!("  {} rows", values.len().to_string().bold());
}

fn render_single_value(val: &Value) {
    match val {
        Value::Object(_) | Value::Array(_) => {
            let pretty = serde_json::to_string_pretty(val).unwrap_or_default();
            for line in pretty.lines() {
                println!("  {}", line);
            }
        }
        _ => {
            println!("  {}", format_value_compact(val).cyan());
        }
    }
}

fn render_value_list(values: &[Value]) {
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL_CONDENSED)
        .set_content_arrangement(ContentArrangement::Dynamic);

    table.set_header(vec![Cell::new("#"), Cell::new("value")]);

    for (i, val) in values.iter().enumerate() {
        table.add_row(vec![Cell::new(i), Cell::new(format_value_compact(val))]);
    }

    println!("{}", table);
    println!("  {} items", values.len().to_string().bold());
}

fn format_value_compact(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            if arr.len() <= 3 {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(format_value_compact)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
                    "[{}, … +{}]",
                    arr.iter()
                        .take(2)
                        .map(format_value_compact)
                        .collect::<Vec<_>>()
                        .join(", "),
                    arr.len() - 2
                )
            }
        }
        Value::Object(map) => {
            if map.len() <= 3 {
                format!(
                    "{{{}}}",
                    map.iter()
                        .map(|(k, v)| format!("{}:{}", k, format_value_compact(v)))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
                    "{{{}, … +{}}}",
                    map.iter()
                        .take(2)
                        .map(|(k, v)| format!("{}:{}", k, format_value_compact(v)))
                        .collect::<Vec<_>>()
                        .join(", "),
                    map.len() - 2
                )
            }
        }
    }
}

pub fn render_keys(keys: &[String], path_hint: &str) {
    if keys.is_empty() {
        println!("  {}", "No keys found.".yellow());
        return;
    }

    println!(
        "  {} at {}\n",
        "Available keys".bold().cyan(),
        if path_hint.is_empty() {
            "$".to_string()
        } else {
            path_hint.to_string()
        }
        .dimmed()
    );

    for key in keys {
        println!("    {} {}", "•".dimmed(), key);
    }
    println!();
    println!(
        "  {} Use {} to extract values.",
        "Tip:".bold(),
        format!(
            "omnirust json path <file> \"$.{}\"",
            keys.first().unwrap_or(&"key".to_string())
        )
        .cyan()
    );
}

pub fn render_syntax_guide(guide: &str) {
    for line in guide.lines() {
        if line.contains("JSONPath Syntax Guide")
            || line.contains("Filter Operators")
            || line.contains("Quick Examples")
        {
            println!("{}", line.bold().cyan());
        } else if line.contains("──") || line.trim().starts_with('#') {
            println!("{}", line.dimmed());
        } else if line.contains("  →  ") || line.contains(" → ") {
            let parts: Vec<&str> = line.splitn(2, '→').collect();
            if parts.len() == 2 {
                print!("{}→ ", parts[0]);
                println!("{}", parts[1].trim().green());
            } else {
                println!("{}", line);
            }
        } else {
            println!("{}", line);
        }
    }
}
