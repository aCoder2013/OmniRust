mod display;
mod engine;
mod jsonpath;
mod timestamp;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use std::time::Instant;

#[derive(Parser)]
#[command(
    name = "omnirust",
    version,
    about = "All your developer tools. One blazingly fast Rust binary."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// JSON analysis powered by DuckDB
    Json {
        #[command(subcommand)]
        action: JsonAction,
    },
    /// Timestamp conversion and utilities
    Ts {
        #[command(subcommand)]
        action: TsAction,
    },
}

#[derive(Subcommand)]
enum TsAction {
    /// Show current timestamp
    Now {
        /// Unit: s, ms, us, ns
        #[arg(short, long, default_value = "s")]
        unit: String,
    },
    /// Convert timestamp to human-readable date
    ToDate {
        /// Integer timestamp value
        timestamp: String,
        /// Unit: s, ms, us, ns (auto-detected if omitted)
        #[arg(short, long)]
        unit: Option<String>,
    },
    /// Convert date string to timestamp
    ToTs {
        /// Date string (RFC3339, "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DD")
        date: String,
        /// Output unit: s, ms, us, ns
        #[arg(short, long, default_value = "s")]
        unit: String,
    },
    /// Calculate duration between two timestamps or dates
    Diff {
        /// First timestamp or date
        a: String,
        /// Second timestamp or date
        b: String,
    },
}

#[derive(Subcommand)]
enum JsonAction {
    /// Auto-detect and display JSON file schema
    Schema {
        /// Path to JSON file
        file: String,
    },
    /// Preview first N rows
    Head {
        /// Path to JSON file
        file: String,
        /// Number of rows
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,
    },
    /// Run SQL query (use 'data' as table name)
    Query {
        /// Path to JSON file
        file: String,
        /// SQL query string
        #[arg(short, long)]
        sql: String,
        /// Max rows to display
        #[arg(short, long, default_value = "100")]
        limit: usize,
    },
    /// Show column statistics via SUMMARIZE
    Stats {
        /// Path to JSON file
        file: String,
        /// Specific columns (comma-separated)
        #[arg(short, long)]
        columns: Option<String>,
    },
    /// Extract data using JSONPath expression
    Path {
        /// Path to JSON file (omit with --syntax to see syntax guide)
        file: Option<String>,
        /// JSONPath expression (e.g. "$[*].name")
        #[arg(short = 'e', long = "expr")]
        expression: Option<String>,
        /// Show JSONPath syntax reference
        #[arg(long)]
        syntax: bool,
    },
    /// List available keys at a JSONPath location
    Keys {
        /// Path to JSON file
        file: String,
        /// JSONPath to inspect (default: root)
        #[arg(short = 'e', long = "expr")]
        expression: Option<String>,
    },
    /// Render a terminal chart
    Chart {
        /// Path to JSON file
        file: String,
        /// Column to visualize
        #[arg(short, long)]
        column: String,
        /// Chart type: bar or hist
        #[arg(short = 't', long, default_value = "bar")]
        chart_type: String,
        /// Number of histogram bins
        #[arg(short, long, default_value = "15")]
        bins: usize,
        /// Max items for bar chart
        #[arg(short, long, default_value = "20")]
        max_items: usize,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Json { action } => handle_json(action),
        Commands::Ts { action } => handle_ts(action),
    }
}

fn handle_json(action: JsonAction) -> Result<()> {
    if let JsonAction::Path {
        syntax: true,
        file: None,
        ..
    } = &action
    {
        display::render_syntax_guide(jsonpath::syntax_guide());
        return Ok(());
    }

    let start = Instant::now();

    let file = match &action {
        JsonAction::Schema { file }
        | JsonAction::Head { file, .. }
        | JsonAction::Query { file, .. }
        | JsonAction::Stats { file, .. }
        | JsonAction::Chart { file, .. }
        | JsonAction::Keys { file, .. } => file.clone(),
        JsonAction::Path { file, syntax, .. } => {
            if *syntax {
                display::render_syntax_guide(jsonpath::syntax_guide());
                return Ok(());
            }
            match file {
                Some(f) => f.clone(),
                None => bail!("Please provide a JSON file. Use --syntax to see JSONPath help."),
            }
        }
    };

    match &action {
        JsonAction::Path {
            expression, syntax, ..
        } => {
            if *syntax {
                display::render_syntax_guide(jsonpath::syntax_guide());
                return Ok(());
            }

            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("Failed to read file: {}", file))?;
            let json: serde_json::Value = serde_json::from_str(&content)
                .with_context(|| format!("Invalid JSON in file: {}", file))?;

            let expr = match expression {
                Some(e) => e.as_str(),
                None => {
                    display::render_syntax_guide(jsonpath::syntax_guide());
                    return Ok(());
                }
            };

            let results = jsonpath::parse_and_eval(&json, expr)?;
            display::render_jsonpath_results(&results, expr);

            let elapsed = start.elapsed();
            eprintln!(
                "\n  {} Completed in {:.3}s",
                "⏱".dimmed(),
                elapsed.as_secs_f64()
            );
            return Ok(());
        }
        JsonAction::Keys { expression, .. } => {
            let content = std::fs::read_to_string(&file)
                .with_context(|| format!("Failed to read file: {}", file))?;
            let json: serde_json::Value = serde_json::from_str(&content)
                .with_context(|| format!("Invalid JSON in file: {}", file))?;

            let keys = jsonpath::list_keys(&json, expression.as_deref())?;
            display::render_keys(&keys, expression.as_deref().unwrap_or("$"));

            let elapsed = start.elapsed();
            eprintln!(
                "\n  {} Completed in {:.3}s",
                "⏱".dimmed(),
                elapsed.as_secs_f64()
            );
            return Ok(());
        }
        _ => {}
    }

    let eng = engine::Engine::new()?;
    eng.register_json(&file)?;

    let row_count = eng.row_count()?;
    eprintln!(
        "  {} Loaded {} ({} rows)\n",
        "✓".green().bold(),
        file.bold(),
        row_count.to_string().cyan()
    );

    match action {
        JsonAction::Schema { .. } => {
            let schema = eng.schema()?;
            display::render_schema(&schema);
        }
        JsonAction::Head { limit, .. } => {
            let result = eng.query("SELECT * FROM data", limit)?;
            display::render_table(&result);
        }
        JsonAction::Query { sql, limit, .. } => {
            let result = eng.query(&sql, limit)?;
            display::render_table(&result);
        }
        JsonAction::Stats { columns, .. } => {
            let cols = columns.map(|c| c.split(',').map(|s| s.trim().to_string()).collect());
            let result = eng.stats(cols)?;
            display::render_table(&result);
        }
        JsonAction::Chart {
            column,
            chart_type,
            bins,
            max_items,
            ..
        } => match chart_type.as_str() {
            "bar" => {
                let data = eng.value_counts(&column, max_items)?;
                display::render_bar_chart(&data, &column);
            }
            "hist" | "histogram" => {
                let data = eng.histogram_data(&column, bins)?;
                display::render_histogram(&data, &column);
            }
            other => bail!("Unknown chart type '{}'. Use 'bar' or 'hist'.", other),
        },
        JsonAction::Path { .. } | JsonAction::Keys { .. } => unreachable!(),
    }

    let elapsed = start.elapsed();
    eprintln!(
        "\n  {} Completed in {:.3}s",
        "⏱".dimmed(),
        elapsed.as_secs_f64()
    );

    Ok(())
}

fn handle_ts(action: TsAction) -> Result<()> {
    match action {
        TsAction::Now { unit } => timestamp::cmd_now(&unit),
        TsAction::ToDate { timestamp, unit } => timestamp::cmd_to_date(&timestamp, unit.as_deref()),
        TsAction::ToTs { date, unit } => timestamp::cmd_to_ts(&date, &unit),
        TsAction::Diff { a, b } => timestamp::cmd_diff(&a, &b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
