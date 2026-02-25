use anyhow::{Context, Result};
use duckdb::Connection;
use std::path::Path;

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
    conn: Connection,
}

impl Engine {
    pub fn new() -> Result<Self> {
        let conn =
            Connection::open_in_memory().context("Failed to create DuckDB in-memory database")?;
        Ok(Self { conn })
    }

    pub fn register_json(&self, file: &str) -> Result<()> {
        let path = Path::new(file)
            .canonicalize()
            .with_context(|| format!("File not found: {}", file))?;
        let path_str = path.to_string_lossy().replace('\'', "''");

        let sql = format!(
            "CREATE OR REPLACE VIEW data AS SELECT * FROM read_json_auto('{}')",
            path_str
        );
        self.conn
            .execute_batch(&sql)
            .with_context(|| format!("Failed to load JSON file: {}", file))?;
        Ok(())
    }

    pub fn schema(&self) -> Result<Vec<ColumnInfo>> {
        let mut stmt = self.conn.prepare("DESCRIBE data")?;
        let rows = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get::<_, String>(0)?,
                dtype: row.get::<_, String>(1)?,
            })
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn query(&self, sql: &str, limit: usize) -> Result<QueryResult> {
        let query_sql = if sql.to_uppercase().contains("LIMIT") {
            sql.to_string()
        } else {
            format!("{} LIMIT {}", sql.trim_end_matches(';'), limit)
        };

        let base_sql = sql.trim_end_matches(';');
        let describe_sql = format!("DESCRIBE {}", base_sql);
        let columns: Vec<ColumnInfo> = match self.conn.prepare(&describe_sql) {
            Ok(mut desc_stmt) => {
                let col_info = desc_stmt.query_map([], |row| {
                    Ok(ColumnInfo {
                        name: row.get::<_, String>(0)?,
                        dtype: row.get::<_, String>(1)?,
                    })
                })?;
                col_info.filter_map(|r| r.ok()).collect()
            }
            Err(_) => Vec::new(),
        };
        let col_count = columns.len();

        let mut stmt = self
            .conn
            .prepare(&query_sql)
            .with_context(|| format!("Invalid SQL: {}", sql))?;

        let rows_iter = stmt.query_map([], |row| {
            let vals: Vec<String> = (0..col_count).map(|i| extract_cell(row, i)).collect();
            Ok(vals)
        })?;

        let mut rows = Vec::new();
        for row in rows_iter {
            rows.push(row?);
        }

        Ok(QueryResult {
            columns,
            rows,
            total_count: None,
        })
    }

    pub fn row_count(&self) -> Result<usize> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM data")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count as usize)
    }

    pub fn stats(&self, columns: Option<Vec<String>>) -> Result<QueryResult> {
        let sql = match columns {
            Some(cols) => {
                let select_cols = cols
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("SUMMARIZE SELECT {} FROM data", select_cols)
            }
            None => "SUMMARIZE data".to_string(),
        };

        let describe_sql = format!(
            "DESCRIBE {}",
            sql.replace("SUMMARIZE", "SELECT * FROM (SUMMARIZE")
        ) + ")";
        let columns: Vec<ColumnInfo> = match self.conn.prepare(&describe_sql) {
            Ok(mut desc_stmt) => {
                let col_info = desc_stmt.query_map([], |row| {
                    Ok(ColumnInfo {
                        name: row.get::<_, String>(0)?,
                        dtype: row.get::<_, String>(1)?,
                    })
                })?;
                col_info.filter_map(|r| r.ok()).collect()
            }
            Err(_) => Vec::new(),
        };
        let col_count = columns.len();

        let mut stmt = self
            .conn
            .prepare(&sql)
            .with_context(|| "Failed to execute SUMMARIZE".to_string())?;

        let rows_iter = stmt.query_map([], |row| {
            let vals: Vec<String> = (0..col_count).map(|i| extract_cell(row, i)).collect();
            Ok(vals)
        })?;

        let mut rows = Vec::new();
        for row in rows_iter {
            rows.push(row?);
        }

        Ok(QueryResult {
            columns,
            rows,
            total_count: None,
        })
    }

    pub fn value_counts(&self, column: &str, max_items: usize) -> Result<Vec<(String, i64)>> {
        let sql = format!(
            "SELECT CAST(\"{}\" AS VARCHAR) as val, COUNT(*) as cnt \
             FROM data \
             WHERE \"{}\" IS NOT NULL \
             GROUP BY \"{}\" \
             ORDER BY cnt DESC \
             LIMIT {}",
            column, column, column, max_items
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let val: String = row.get(0)?;
            let cnt: i64 = row.get(1)?;
            Ok((val, cnt))
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn histogram_data(&self, column: &str, bins: usize) -> Result<Vec<(f64, f64, i64)>> {
        let sql = format!(
            "WITH bounds AS ( \
                SELECT MIN(\"{col}\")::DOUBLE as mn, MAX(\"{col}\")::DOUBLE as mx FROM data \
                WHERE \"{col}\" IS NOT NULL \
             ), \
             params AS ( \
                SELECT mn, mx, (mx - mn) / {bins}.0 as bin_width FROM bounds \
             ), \
             binned AS ( \
                SELECT \
                    CASE \
                        WHEN bin_width = 0 THEN 0 \
                        ELSE LEAST(FLOOR((\"{col}\"::DOUBLE - mn) / bin_width), {bins} - 1) \
                    END as bin_idx, \
                    COUNT(*) as cnt \
                FROM data, params \
                WHERE \"{col}\" IS NOT NULL \
                GROUP BY bin_idx \
                ORDER BY bin_idx \
             ) \
             SELECT \
                mn + bin_idx * bin_width as bin_start, \
                mn + (bin_idx + 1) * bin_width as bin_end, \
                cnt \
             FROM binned, params \
             ORDER BY bin_start",
            col = column,
            bins = bins
        );

        let mut stmt = self
            .conn
            .prepare(&sql)
            .with_context(|| format!("Column '{}' may not be numeric", column))?;
        let rows = stmt.query_map([], |row| {
            let start: f64 = row.get(0)?;
            let end: f64 = row.get(1)?;
            let cnt: i64 = row.get(2)?;
            Ok((start, end, cnt))
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }
}

fn extract_cell(row: &duckdb::Row, idx: usize) -> String {
    if let Ok(v) = row.get::<_, String>(idx) {
        return v;
    }
    if let Ok(v) = row.get::<_, i64>(idx) {
        return v.to_string();
    }
    if let Ok(v) = row.get::<_, f64>(idx) {
        if v.fract() == 0.0 && v.abs() < 1e15 {
            return format!("{:.0}", v);
        }
        return format!("{}", v);
    }
    if let Ok(v) = row.get::<_, bool>(idx) {
        return v.to_string();
    }
    "NULL".to_string()
}
