use anyhow::{bail, Result};
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use colored::Colorize;

#[derive(Debug, Clone, Copy)]
pub enum TsUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

impl TsUnit {
    pub fn from_str_loose(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "s" | "sec" | "secs" | "second" | "seconds" => Ok(Self::Seconds),
            "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => Ok(Self::Milliseconds),
            "us" | "µs" | "micro" | "micros" | "microsecond" | "microseconds" => {
                Ok(Self::Microseconds)
            }
            "ns" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => Ok(Self::Nanoseconds),
            other => bail!("Unknown unit '{}'. Use s/ms/us/ns.", other),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Seconds => "s",
            Self::Milliseconds => "ms",
            Self::Microseconds => "µs",
            Self::Nanoseconds => "ns",
        }
    }
}

fn guess_unit(ts: i64) -> TsUnit {
    let abs = ts.unsigned_abs();
    if abs < 1_000_000_000_000 {
        TsUnit::Seconds
    } else if abs < 1_000_000_000_000_000 {
        TsUnit::Milliseconds
    } else if abs < 1_000_000_000_000_000_000 {
        TsUnit::Microseconds
    } else {
        TsUnit::Nanoseconds
    }
}

fn ts_to_utc(ts: i64, unit: TsUnit) -> Result<DateTime<Utc>> {
    let dt = match unit {
        TsUnit::Seconds => Utc
            .timestamp_opt(ts, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?,
        TsUnit::Milliseconds => {
            let secs = ts / 1_000;
            let nsecs = ((ts % 1_000) * 1_000_000) as u32;
            Utc.timestamp_opt(secs, nsecs)
                .single()
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?
        }
        TsUnit::Microseconds => {
            let secs = ts / 1_000_000;
            let nsecs = ((ts % 1_000_000) * 1_000) as u32;
            Utc.timestamp_opt(secs, nsecs)
                .single()
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?
        }
        TsUnit::Nanoseconds => {
            let secs = ts / 1_000_000_000;
            let nsecs = (ts % 1_000_000_000) as u32;
            Utc.timestamp_opt(secs, nsecs)
                .single()
                .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", ts))?
        }
    };
    Ok(dt)
}

fn format_relative(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    let duration = to.signed_duration_since(from);
    let total_secs = duration.num_seconds();

    if total_secs == 0 {
        return "just now".to_string();
    }

    let (abs_secs, suffix) = if total_secs > 0 {
        (total_secs, "ago")
    } else {
        (-total_secs, "from now")
    };

    if abs_secs < 60 {
        return format!("{}s {}", abs_secs, suffix);
    }
    let mins = abs_secs / 60;
    if mins < 60 {
        return format!("{}m {}s {}", mins, abs_secs % 60, suffix);
    }
    let hours = mins / 60;
    if hours < 24 {
        return format!("{}h {}m {}", hours, mins % 60, suffix);
    }
    let days = hours / 24;
    if days < 365 {
        return format!("{}d {}h {}", days, hours % 24, suffix);
    }
    let years = days / 365;
    format!("{}y {}d {}", years, days % 365, suffix)
}

pub fn cmd_now(unit: &str) -> Result<()> {
    let unit = TsUnit::from_str_loose(unit)?;
    let now_utc = Utc::now();
    let now_local = Local::now();

    let ts_val: i64 = match unit {
        TsUnit::Seconds => now_utc.timestamp(),
        TsUnit::Milliseconds => now_utc.timestamp_millis(),
        TsUnit::Microseconds => now_utc.timestamp_micros(),
        TsUnit::Nanoseconds => now_utc
            .timestamp_nanos_opt()
            .unwrap_or(now_utc.timestamp_micros() * 1_000),
    };

    println!(
        "  {}  {}",
        "Timestamp".bold(),
        format!("{} ({})", ts_val, unit.label()).cyan()
    );
    println!(
        "  {}        {}",
        "UTC".bold(),
        now_utc.format("%Y-%m-%d %H:%M:%S%.3f %Z")
    );
    println!(
        "  {}      {}",
        "Local".bold(),
        now_local.format("%Y-%m-%d %H:%M:%S%.3f %:z")
    );
    println!(
        "  {}    {}",
        "ISO 8601".bold(),
        now_utc.to_rfc3339().dimmed()
    );

    Ok(())
}

pub fn cmd_to_date(ts_str: &str, unit: Option<&str>) -> Result<()> {
    let ts: i64 = ts_str
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("'{}' is not a valid integer timestamp", ts_str))?;

    let resolved_unit = match unit {
        Some(u) => TsUnit::from_str_loose(u)?,
        None => {
            let guessed = guess_unit(ts);
            eprintln!(
                "  {} Auto-detected unit: {}\n",
                "ℹ".blue(),
                guessed.label().yellow()
            );
            guessed
        }
    };

    let dt_utc = ts_to_utc(ts, resolved_unit)?;
    let dt_local: DateTime<Local> = dt_utc.into();
    let now = Utc::now();
    let relative = format_relative(dt_utc, now);

    println!(
        "  {}      {} ({})",
        "Input".bold(),
        ts_str.cyan(),
        resolved_unit.label()
    );
    println!(
        "  {}        {}",
        "UTC".bold(),
        dt_utc.format("%Y-%m-%d %H:%M:%S%.3f %Z")
    );
    println!(
        "  {}      {}",
        "Local".bold(),
        dt_local.format("%Y-%m-%d %H:%M:%S%.3f %:z")
    );
    println!(
        "  {}    {}",
        "ISO 8601".bold(),
        dt_utc.to_rfc3339().dimmed()
    );
    println!("  {}   {}", "Relative".bold(), relative.green());

    Ok(())
}

pub fn cmd_to_ts(date_str: &str, unit: &str) -> Result<()> {
    let unit = TsUnit::from_str_loose(unit)?;

    let dt_utc = try_parse_datetime(date_str)?;
    let dt_local: DateTime<Local> = dt_utc.into();

    let ts_val: i64 = match unit {
        TsUnit::Seconds => dt_utc.timestamp(),
        TsUnit::Milliseconds => dt_utc.timestamp_millis(),
        TsUnit::Microseconds => dt_utc.timestamp_micros(),
        TsUnit::Nanoseconds => dt_utc
            .timestamp_nanos_opt()
            .unwrap_or(dt_utc.timestamp_micros() * 1_000),
    };

    println!("  {}      {}", "Input".bold(), date_str.cyan());
    println!(
        "  {}  {} ({})",
        "Timestamp".bold(),
        ts_val.to_string().yellow(),
        unit.label()
    );
    println!(
        "  {}        {}",
        "UTC".bold(),
        dt_utc.format("%Y-%m-%d %H:%M:%S%.3f %Z")
    );
    println!(
        "  {}      {}",
        "Local".bold(),
        dt_local.format("%Y-%m-%d %H:%M:%S%.3f %:z")
    );

    Ok(())
}

pub fn cmd_diff(a: &str, b: &str) -> Result<()> {
    let dt_a = parse_flexible(a)?;
    let dt_b = parse_flexible(b)?;

    let duration = dt_b.signed_duration_since(dt_a);
    let total_ms = duration.num_milliseconds();
    let total_secs = duration.num_seconds();
    let total_mins = duration.num_minutes();
    let total_hours = duration.num_hours();
    let total_days = duration.num_days();

    println!("  {}        {}", "A".bold(), format_dt_or_ts(a, &dt_a));
    println!("  {}        {}", "B".bold(), format_dt_or_ts(b, &dt_b));
    println!();
    println!(
        "  {}     {} B - A",
        "Direction".bold(),
        if total_ms >= 0 { "→" } else { "←" }
    );
    println!();

    let abs_ms = total_ms.unsigned_abs();
    let abs_secs = total_secs.unsigned_abs();

    if abs_ms < 1_000 {
        println!(
            "  {}  {} ms",
            "Duration".bold(),
            total_ms.to_string().cyan()
        );
    } else if abs_secs < 60 {
        println!(
            "  {}  {}.{:03} seconds",
            "Duration".bold(),
            total_secs,
            (abs_ms % 1_000)
        );
    } else {
        let sign = if total_secs < 0 { "-" } else { "" };
        let d = abs_secs / 86400;
        let h = (abs_secs % 86400) / 3600;
        let m = (abs_secs % 3600) / 60;
        let s = abs_secs % 60;

        let mut parts = Vec::new();
        if d > 0 {
            parts.push(format!("{}d", d));
        }
        if h > 0 || d > 0 {
            parts.push(format!("{}h", h));
        }
        if m > 0 || h > 0 || d > 0 {
            parts.push(format!("{}m", m));
        }
        parts.push(format!("{}s", s));

        println!(
            "  {}  {}",
            "Duration".bold(),
            format!("{}{}", sign, parts.join(" ")).cyan()
        );
    }

    println!();
    println!("  {}   {} days", "In days".bold(), total_days);
    println!("  {}  {} hours", "In hours".bold(), total_hours);
    println!("  {}   {} mins", "In mins".bold(), total_mins);
    println!("  {}   {} secs", "In secs".bold(), total_secs);
    println!("  {}     {} ms", "In ms".bold(), total_ms);

    Ok(())
}

fn try_parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    let formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ];

    for fmt in &formats {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(Utc.from_utc_datetime(&ndt));
        }
    }

    if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let ndt = nd.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Utc.from_utc_datetime(&ndt));
    }

    bail!(
        "Cannot parse '{}'. Supported formats: RFC3339, YYYY-MM-DD HH:MM:SS, YYYY-MM-DD",
        s
    )
}

fn parse_flexible(s: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = s.parse::<i64>() {
        let unit = guess_unit(ts);
        return ts_to_utc(ts, unit);
    }
    try_parse_datetime(s)
}

fn format_dt_or_ts(original: &str, dt: &DateTime<Utc>) -> String {
    if original.parse::<i64>().is_ok() {
        format!(
            "{} → {}",
            original.cyan(),
            dt.format("%Y-%m-%d %H:%M:%S UTC")
        )
    } else {
        format!(
            "{} ({})",
            original.cyan(),
            dt.format("%Y-%m-%d %H:%M:%S UTC")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guess_unit_seconds() {
        assert!(matches!(guess_unit(1700000000), TsUnit::Seconds));
    }

    #[test]
    fn test_guess_unit_millis() {
        assert!(matches!(guess_unit(1700000000000), TsUnit::Milliseconds));
    }

    #[test]
    fn test_guess_unit_micros() {
        assert!(matches!(guess_unit(1700000000000000), TsUnit::Microseconds));
    }

    #[test]
    fn test_guess_unit_nanos() {
        assert!(matches!(
            guess_unit(1700000000000000000),
            TsUnit::Nanoseconds
        ));
    }

    #[test]
    fn test_ts_to_utc_seconds() {
        let dt = ts_to_utc(0, TsUnit::Seconds).unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "1970-01-01");
    }

    #[test]
    fn test_ts_to_utc_millis() {
        let dt = ts_to_utc(1_700_000_000_000, TsUnit::Milliseconds).unwrap();
        assert_eq!(dt.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_parse_datetime_rfc3339() {
        let dt = try_parse_datetime("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-15 10:30:00"
        );
    }

    #[test]
    fn test_parse_datetime_date_only() {
        let dt = try_parse_datetime("2024-01-15").unwrap();
        assert_eq!(
            dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-15 00:00:00"
        );
    }

    #[test]
    fn test_parse_datetime_full() {
        let dt = try_parse_datetime("2024-01-15 10:30:00").unwrap();
        assert_eq!(dt.format("%H:%M:%S").to_string(), "10:30:00");
    }

    #[test]
    fn test_format_relative_seconds() {
        let a = Utc::now() - chrono::Duration::seconds(30);
        let rel = format_relative(a, Utc::now());
        assert!(rel.contains("30s"));
        assert!(rel.contains("ago"));
    }

    #[test]
    fn test_format_relative_hours() {
        let a = Utc::now() - chrono::Duration::hours(3);
        let rel = format_relative(a, Utc::now());
        assert!(rel.contains("3h"));
    }

    #[test]
    fn test_parse_flexible_int() {
        let dt = parse_flexible("1700000000").unwrap();
        assert_eq!(dt.timestamp(), 1_700_000_000);
    }

    #[test]
    fn test_parse_flexible_string() {
        let dt = parse_flexible("2024-06-15 12:00:00").unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-06-15");
    }
}
