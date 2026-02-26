use anyhow::{bail, Context, Result};
use arrow_json::ReaderBuilder;
use colored::Colorize;
use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use serde_json::Value;
use std::io::Cursor;
use std::sync::Arc;

pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<String>>,
    pub total_count: Option<usize>,
}

pub struct ColumnInfo {
    pub name: String,
    pub dtype: String,
}

pub struct Engine {
    ctx: SessionContext,
    rt: tokio::runtime::Runtime,
}

impl Engine {
    pub fn new() -> Result<Self> {
        let rt = tokio::runtime::Runtime::new().context("Failed to create async runtime")?;
        let ctx = SessionContext::new();
        Ok(Self { ctx, rt })
    }

    pub fn register_json(&self, file_path: &str, root: Option<&str>) -> Result<()> {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("File not found: {}", file_path))?;
        self.register_json_content(&content, root)
    }

    pub fn register_json_content(&self, content: &str, root: Option<&str>) -> Result<()> {
        let parsed: Value = serde_json::from_str(content.trim()).context("Invalid JSON")?;

        let array_data = extract_array(&parsed, root)?;

        let ndjson = values_to_ndjson(&array_data)?;

        if ndjson.is_empty() {
            bail!("No data rows found");
        }

        let schema = {
            let mut cursor = Cursor::new(&ndjson);
            let (schema, _) = arrow_json::reader::infer_json_schema(&mut cursor, None)
                .context("Failed to infer JSON schema")?;
            Arc::new(schema)
        };

        let reader = ReaderBuilder::new(schema.clone())
            .with_batch_size(8192)
            .build(Cursor::new(&ndjson))
            .context("Failed to build JSON reader")?;

        let batches: Vec<_> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("Failed to read JSON data")?;

        if batches.is_empty() {
            bail!("No data found in JSON");
        }

        let table =
            MemTable::try_new(schema, vec![batches]).context("Failed to create in-memory table")?;

        self.rt.block_on(async {
            self.ctx
                .register_table("data", Arc::new(table))
                .map_err(|e| anyhow::anyhow!("Failed to register table: {}", e))
        })?;

        Ok(())
    }

    pub fn schema(&self) -> Result<Vec<ColumnInfo>> {
        self.rt.block_on(async {
            let df = self
                .ctx
                .table("data")
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            let schema = df.schema();

            Ok(schema
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    dtype: format!("{}", f.data_type()),
                })
                .collect())
        })
    }

    pub fn query(&self, sql: &str, limit: usize) -> Result<QueryResult> {
        let query_sql = if sql.to_uppercase().contains("LIMIT") {
            sql.to_string()
        } else {
            format!("{} LIMIT {}", sql.trim_end_matches(';'), limit)
        };

        self.rt.block_on(async {
            let df = self
                .ctx
                .sql(&query_sql)
                .await
                .with_context(|| format!("Invalid SQL: {}", sql))?;
            let schema = df.schema().clone();
            let batches = df.collect().await.context("Failed to execute query")?;

            let columns: Vec<ColumnInfo> = schema
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    dtype: format!("{}", f.data_type()),
                })
                .collect();

            let mut rows = Vec::new();
            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let row: Vec<String> = (0..batch.num_columns())
                        .map(|col_idx| extract_cell(batch.column(col_idx), row_idx))
                        .collect();
                    rows.push(row);
                }
            }

            Ok(QueryResult {
                columns,
                rows,
                total_count: None,
            })
        })
    }

    pub fn row_count(&self) -> Result<usize> {
        self.rt.block_on(async {
            let df = self
                .ctx
                .sql("SELECT COUNT(*) AS cnt FROM data")
                .await
                .context("Failed to count rows")?;
            let batches = df.collect().await?;

            if let Some(batch) = batches.first() {
                if batch.num_rows() > 0 {
                    let val = extract_cell(batch.column(0), 0);
                    return Ok(val.parse::<usize>().unwrap_or(0));
                }
            }
            Ok(0)
        })
    }

    pub fn stats(&self, columns: Option<Vec<String>>) -> Result<QueryResult> {
        self.rt.block_on(async {
            let df = self
                .ctx
                .table("data")
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            let schema = df.schema().clone();

            let col_names: Vec<String> = match columns {
                Some(c) => c,
                None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            };

            let mut stat_rows: Vec<Vec<String>> = Vec::new();

            for col_name in &col_names {
                let field = schema
                    .field_with_name(None, col_name)
                    .map_err(|e| anyhow::anyhow!("Column '{}' not found: {}", col_name, e))?;
                let is_numeric = field.data_type().is_numeric();

                let sql = if is_numeric {
                    format!(
                        "SELECT \
                            CAST(MIN(\"{c}\") AS VARCHAR), \
                            CAST(MAX(\"{c}\") AS VARCHAR), \
                            CAST(COUNT(DISTINCT \"{c}\") AS VARCHAR), \
                            CAST(ROUND(AVG(CAST(\"{c}\" AS DOUBLE)), 2) AS VARCHAR), \
                            CAST(ROUND(STDDEV(CAST(\"{c}\" AS DOUBLE)), 2) AS VARCHAR), \
                            CAST(COUNT(*) AS VARCHAR), \
                            CAST(ROUND(100.0 * CAST(COUNT(*) - COUNT(\"{c}\") AS DOUBLE) / COUNT(*), 1) AS VARCHAR) \
                        FROM data",
                        c = col_name
                    )
                } else {
                    format!(
                        "SELECT \
                            CAST(MIN(\"{c}\") AS VARCHAR) AS min_val, \
                            CAST(MAX(\"{c}\") AS VARCHAR) AS max_val, \
                            CAST(COUNT(DISTINCT \"{c}\") AS VARCHAR) AS uniq, \
                            'NULL' AS avg_val, \
                            'NULL' AS std_val, \
                            CAST(COUNT(*) AS VARCHAR) AS cnt, \
                            CAST(ROUND(100.0 * CAST(COUNT(*) - COUNT(\"{c}\") AS DOUBLE) / COUNT(*), 1) AS VARCHAR) AS null_pct \
                        FROM data",
                        c = col_name
                    )
                };

                let df = self.ctx.sql(&sql).await?;
                let batches = df.collect().await?;

                if let Some(batch) = batches.first() {
                    if batch.num_rows() > 0 {
                        let mut row = vec![col_name.clone(), format!("{}", field.data_type())];
                        for col_idx in 0..batch.num_columns() {
                            row.push(extract_cell(batch.column(col_idx), 0));
                        }
                        stat_rows.push(row);
                    }
                }
            }

            let stat_columns = vec![
                ci("column_name"),
                ci("column_type"),
                ci("min"),
                ci("max"),
                ci("approx_unique"),
                ci("avg"),
                ci("std"),
                ci("count"),
                ci("null_percentage"),
            ];

            Ok(QueryResult {
                columns: stat_columns,
                rows: stat_rows,
                total_count: None,
            })
        })
    }

    pub fn value_counts(&self, column: &str, max_items: usize) -> Result<Vec<(String, i64)>> {
        let sql = format!(
            "SELECT CAST(\"{}\" AS VARCHAR) AS val, COUNT(*) AS cnt \
             FROM data \
             WHERE \"{}\" IS NOT NULL \
             GROUP BY CAST(\"{}\" AS VARCHAR) \
             ORDER BY cnt DESC \
             LIMIT {}",
            column, column, column, max_items
        );

        self.rt.block_on(async {
            let df = self.ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let mut result = Vec::new();

            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let val = extract_cell(batch.column(0), row_idx);
                    let cnt: i64 = extract_cell(batch.column(1), row_idx).parse().unwrap_or(0);
                    result.push((val, cnt));
                }
            }
            Ok(result)
        })
    }

    pub fn histogram_data(&self, column: &str, bins: usize) -> Result<Vec<(f64, f64, i64)>> {
        let sql = format!(
            "WITH bounds AS ( \
                SELECT MIN(CAST(\"{col}\" AS DOUBLE)) AS mn, MAX(CAST(\"{col}\" AS DOUBLE)) AS mx \
                FROM data WHERE \"{col}\" IS NOT NULL \
             ), \
             params AS ( \
                SELECT mn, mx, (mx - mn) / {bins}.0 AS bin_width FROM bounds \
             ), \
             binned AS ( \
                SELECT \
                    CASE \
                        WHEN params.bin_width = 0 THEN 0 \
                        ELSE CAST(FLOOR((CAST(\"{col}\" AS DOUBLE) - params.mn) / params.bin_width) AS BIGINT) \
                    END AS bin_idx, \
                    COUNT(*) AS cnt \
                FROM data CROSS JOIN params \
                WHERE \"{col}\" IS NOT NULL \
                GROUP BY bin_idx \
                ORDER BY bin_idx \
             ) \
             SELECT \
                params.mn + CAST(binned.bin_idx AS DOUBLE) * params.bin_width AS bin_start, \
                params.mn + (CAST(binned.bin_idx AS DOUBLE) + 1) * params.bin_width AS bin_end, \
                binned.cnt \
             FROM binned CROSS JOIN params \
             ORDER BY bin_start",
            col = column,
            bins = bins
        );

        self.rt.block_on(async {
            let df = self
                .ctx
                .sql(&sql)
                .await
                .with_context(|| format!("Column '{}' may not be numeric", column))?;
            let batches = df.collect().await?;
            let mut result = Vec::new();

            for batch in &batches {
                for row_idx in 0..batch.num_rows() {
                    let start: f64 = extract_cell(batch.column(0), row_idx)
                        .parse()
                        .unwrap_or(0.0);
                    let end: f64 = extract_cell(batch.column(1), row_idx)
                        .parse()
                        .unwrap_or(0.0);
                    let cnt: i64 = extract_cell(batch.column(2), row_idx).parse().unwrap_or(0);
                    result.push((start, end, cnt));
                }
            }
            Ok(result)
        })
    }
}

fn extract_array(val: &Value, root: Option<&str>) -> Result<Vec<Value>> {
    let target = match root {
        Some(path) => navigate(val, path)?,
        None => val.clone(),
    };

    match target {
        Value::Array(arr) => Ok(arr),
        Value::Object(_) => {
            if root.is_some() {
                Ok(vec![target])
            } else {
                match find_best_array(&target) {
                    Some((path, arr)) => {
                        eprintln!(
                            "  {} Auto-detected data array at {} ({} items)",
                            "ℹ".blue(),
                            format!("\"{}\"", path).yellow(),
                            arr.len().to_string().cyan()
                        );
                        eprintln!(
                            "  {} Use {} to specify explicitly\n",
                            "Tip:".bold(),
                            format!("--root {}", path).cyan()
                        );
                        Ok(arr)
                    }
                    None => Ok(vec![target]),
                }
            }
        }
        Value::Null => bail!("JSON value is null"),
        _ => Ok(vec![target]),
    }
}

fn navigate(val: &Value, path: &str) -> Result<Value> {
    let path = path.trim().trim_start_matches('$').trim_start_matches('.');
    if path.is_empty() {
        return Ok(val.clone());
    }

    let mut current = val;
    for segment in split_path(path) {
        match current {
            Value::Object(map) => match map.get(&segment) {
                Some(v) => current = v,
                None => bail!(
                    "Key '{}' not found. Available keys: {}",
                    segment,
                    map.keys()
                        .map(|k| k.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            },
            Value::Array(arr) => {
                if let Ok(idx) = segment.parse::<usize>() {
                    match arr.get(idx) {
                        Some(v) => current = v,
                        None => bail!("Index {} out of range (length {})", idx, arr.len()),
                    }
                } else {
                    let extracted: Vec<Value> = arr
                        .iter()
                        .filter_map(|item| item.get(&segment).cloned())
                        .collect();
                    if extracted.is_empty() {
                        bail!("Key '{}' not found in array elements", segment);
                    }
                    return Ok(Value::Array(extracted));
                }
            }
            _ => bail!(
                "Cannot navigate into {} at '{}'",
                type_name(current),
                segment
            ),
        }
    }
    Ok(current.clone())
}

fn split_path(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut in_bracket = false;

    for ch in path.chars() {
        match ch {
            '.' if !in_bracket => {
                if !current.is_empty() {
                    segments.push(current.clone());
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(current.clone());
                    current.clear();
                }
                in_bracket = true;
            }
            ']' => {
                in_bracket = false;
                if !current.is_empty() {
                    let key = current.trim_matches(|c| c == '\'' || c == '"').to_string();
                    segments.push(key);
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

fn find_best_array(val: &Value) -> Option<(String, Vec<Value>)> {
    if let Value::Object(map) = val {
        let mut best: Option<(String, Vec<Value>)> = None;

        for (key, v) in map {
            if let Value::Array(arr) = v {
                if !arr.is_empty() && arr.iter().any(|item| item.is_object()) {
                    let is_better = match &best {
                        None => true,
                        Some((_, existing)) => arr.len() > existing.len(),
                    };
                    if is_better {
                        best = Some((key.clone(), arr.clone()));
                    }
                }
            }

            if let Value::Object(_) = v {
                if let Some((sub_path, arr)) = find_best_array(v) {
                    let full_path = format!("{}.{}", key, sub_path);
                    let is_better = match &best {
                        None => true,
                        Some((_, existing)) => arr.len() > existing.len(),
                    };
                    if is_better {
                        best = Some((full_path, arr));
                    }
                }
            }
        }

        best
    } else {
        None
    }
}

fn type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn values_to_ndjson(values: &[Value]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for item in values {
        serde_json::to_writer(&mut buf, item)?;
        buf.push(b'\n');
    }
    Ok(buf)
}

fn extract_cell(col: &dyn Array, row: usize) -> String {
    if col.is_null(row) {
        return "NULL".to_string();
    }
    array_value_to_string(col, row).unwrap_or_else(|_| "NULL".to_string())
}

fn ci(name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        dtype: String::new(),
    }
}
