use anyhow::{bail, Context, Result};
use arrow_json::ReaderBuilder;
use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
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

    pub fn register_json(&self, file_path: &str) -> Result<()> {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("File not found: {}", file_path))?;

        let ndjson = to_ndjson(&content)?;
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
            bail!("No data found in JSON file");
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
            let df = self.ctx.table("data").await
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
                        let mut row = vec![
                            col_name.clone(),
                            format!("{}", field.data_type()),
                        ];
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

fn to_ndjson(content: &str) -> Result<Vec<u8>> {
    let trimmed = content.trim();
    if trimmed.starts_with('[') {
        let arr: Vec<serde_json::Value> =
            serde_json::from_str(trimmed).context("Invalid JSON array")?;
        let mut buf = Vec::new();
        for item in &arr {
            serde_json::to_writer(&mut buf, item)?;
            buf.push(b'\n');
        }
        Ok(buf)
    } else if trimmed.contains('\n') && !trimmed.starts_with('{') {
        Ok(trimmed.as_bytes().to_vec())
    } else {
        let mut buf = Vec::new();
        for line in trimmed.lines() {
            let line = line.trim();
            if !line.is_empty() {
                let _: serde_json::Value = serde_json::from_str(line).context("Invalid JSON")?;
                buf.extend_from_slice(line.as_bytes());
                buf.push(b'\n');
            }
        }
        Ok(buf)
    }
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
