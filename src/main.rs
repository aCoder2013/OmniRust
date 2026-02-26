mod display;
mod engine;
mod export;
mod input;
mod jsonpath;
mod jsonutil;
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
    /// Auto-detect and display JSON file schema (use '-' for stdin)
    Schema {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// Root path to data array (e.g. "result.dataList")
        #[arg(short, long)]
        root: Option<String>,
    },
    /// Preview first N rows
    Head {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// Number of rows
        #[arg(short = 'n', long, default_value = "10")]
        limit: usize,
        /// Root path to data array (e.g. "result.dataList")
        #[arg(short, long)]
        root: Option<String>,
    },
    /// Run SQL query (use 'data' as table name)
    Query {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// SQL query string
        #[arg(short, long)]
        sql: String,
        /// Max rows to display
        #[arg(short, long, default_value = "100")]
        limit: usize,
        /// Output format: table, csv, json, jsonl, md
        #[arg(short, long, default_value = "table")]
        output: String,
        /// Export to file path (infers format from extension)
        #[arg(short = 'O', long)]
        outfile: Option<String>,
        /// Root path to data array (e.g. "result.dataList")
        #[arg(short, long)]
        root: Option<String>,
    },
    /// Show column statistics via SUMMARIZE
    Stats {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// Specific columns (comma-separated)
        #[arg(short, long)]
        columns: Option<String>,
        /// Output format: table, csv, json, jsonl, md
        #[arg(short, long, default_value = "table")]
        output: String,
        /// Root path to data array (e.g. "result.dataList")
        #[arg(long)]
        root: Option<String>,
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
        /// Path to JSON file, or '-' for stdin
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
        /// Root path to data array (e.g. "result.dataList")
        #[arg(long)]
        root: Option<String>,
    },
    /// Pretty-print JSON with indentation
    Pretty {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// Indentation spaces
        #[arg(short, long, default_value = "2")]
        indent: usize,
    },
    /// Minify JSON (remove whitespace)
    Minify {
        /// Path to JSON file, or '-' for stdin
        file: String,
    },
    /// Validate JSON and show structure info
    Validate {
        /// Path to JSON file, or '-' for stdin
        file: String,
    },
    /// Flatten nested JSON into dot-notation keys
    Flatten {
        /// Path to JSON file, or '-' for stdin
        file: String,
        /// Key separator
        #[arg(short = 'd', long, default_value = ".")]
        separator: String,
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

    match &action {
        JsonAction::Pretty { file, indent } => {
            let content = input::read_content(file)?;
            jsonutil::cmd_pretty(&content, *indent)?;
            return Ok(());
        }
        JsonAction::Minify { file } => {
            let content = input::read_content(file)?;
            jsonutil::cmd_minify(&content)?;
            return Ok(());
        }
        JsonAction::Validate { file } => {
            let content = input::read_content(file)?;
            jsonutil::cmd_validate(&content, file)?;
            return Ok(());
        }
        JsonAction::Flatten { file, separator } => {
            let content = input::read_content(file)?;
            jsonutil::cmd_flatten(&content, separator)?;
            return Ok(());
        }
        JsonAction::Path {
            file,
            expression,
            syntax,
        } => {
            if *syntax {
                display::render_syntax_guide(jsonpath::syntax_guide());
                return Ok(());
            }
            let file = match file {
                Some(f) => f.clone(),
                None => bail!("Please provide a JSON file. Use --syntax to see JSONPath help."),
            };
            let content = input::read_content(&file)?;
            let json: serde_json::Value = serde_json::from_str(&content)
                .with_context(|| format!("Invalid JSON in: {}", file))?;

            let expr = match expression {
                Some(e) => e.as_str(),
                None => {
                    display::render_syntax_guide(jsonpath::syntax_guide());
                    return Ok(());
                }
            };

            let results = jsonpath::parse_and_eval(&json, expr)?;
            display::render_jsonpath_results(&results, expr);
            eprintln!(
                "\n  {} Completed in {:.3}s",
                "⏱".dimmed(),
                start.elapsed().as_secs_f64()
            );
            return Ok(());
        }
        JsonAction::Keys { file, expression } => {
            let content = input::read_content(file)?;
            let json: serde_json::Value = serde_json::from_str(&content)
                .with_context(|| format!("Invalid JSON in: {}", file))?;

            let keys = jsonpath::list_keys(&json, expression.as_deref())?;
            display::render_keys(&keys, expression.as_deref().unwrap_or("$"));
            eprintln!(
                "\n  {} Completed in {:.3}s",
                "⏱".dimmed(),
                start.elapsed().as_secs_f64()
            );
            return Ok(());
        }
        _ => {}
    }

    let (file_arg, root_arg) = match &action {
        JsonAction::Schema { file, root } => (file.as_str(), root.as_deref()),
        JsonAction::Head { file, root, .. } => (file.as_str(), root.as_deref()),
        JsonAction::Query { file, root, .. } => (file.as_str(), root.as_deref()),
        JsonAction::Stats { file, root, .. } => (file.as_str(), root.as_deref()),
        JsonAction::Chart { file, root, .. } => (file.as_str(), root.as_deref()),
        _ => unreachable!(),
    };

    let source = input::resolve_input(file_arg)?;
    let eng = engine::Engine::new()?;
    eng.register_json(source.path(), root_arg)?;

    let row_count = eng.row_count()?;
    eprintln!(
        "  {} Loaded {} ({} rows)\n",
        "✓".green().bold(),
        source.label().bold(),
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
        JsonAction::Query {
            sql,
            limit,
            output,
            outfile,
            ..
        } => {
            let result = eng.query(&sql, limit)?;
            let fmt_str = outfile
                .as_deref()
                .filter(|_| output == "table")
                .unwrap_or(&output);
            let fmt = export::OutputFormat::from_str_or_path(fmt_str)?;

            match fmt {
                export::OutputFormat::Table => display::render_table(&result),
                _ => export::export_result(&result, &fmt, outfile.as_deref())?,
            }
        }
        JsonAction::Stats {
            columns, output, ..
        } => {
            let cols = columns.map(|c| c.split(',').map(|s| s.trim().to_string()).collect());
            let result = eng.stats(cols)?;
            let fmt = export::OutputFormat::from_str_or_path(&output)?;
            match fmt {
                export::OutputFormat::Table => display::render_table(&result),
                _ => export::export_result(&result, &fmt, None)?,
            }
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
        _ => unreachable!(),
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
