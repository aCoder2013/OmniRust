use crate::engine::{ColumnInfo, QueryResult};
use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, ContentArrangement, Table};

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
