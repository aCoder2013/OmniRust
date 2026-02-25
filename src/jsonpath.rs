use anyhow::{bail, Result};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Root,
    Field(String),
    Index(i64),
    Wildcard,
    Slice(Option<i64>, Option<i64>),
    RecursiveDescent,
    Filter(String),
}

pub fn parse_and_eval(json: &Value, expr: &str) -> Result<Vec<Value>> {
    let tokens = parse(expr)?;
    Ok(evaluate(json, &tokens))
}

pub fn list_keys(json: &Value, expr: Option<&str>) -> Result<Vec<String>> {
    let target = match expr {
        Some(e) => {
            let results = parse_and_eval(json, e)?;
            match results.into_iter().next() {
                Some(v) => v,
                None => return Ok(vec![]),
            }
        }
        None => json.clone(),
    };

    match &target {
        Value::Object(map) => Ok(map.keys().cloned().collect()),
        Value::Array(arr) => {
            if let Some(Value::Object(map)) = arr.first() {
                Ok(map.keys().cloned().collect())
            } else {
                Ok(vec![format!("[0..{}]", arr.len())])
            }
        }
        _ => Ok(vec![target.to_string()]),
    }
}

fn parse(expr: &str) -> Result<Vec<Token>> {
    let expr = expr.trim();
    if expr.is_empty() {
        bail!("Empty JSONPath expression");
    }

    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;

    if i < chars.len() && chars[i] == '$' {
        tokens.push(Token::Root);
        i += 1;
    } else {
        tokens.push(Token::Root);
    }

    while i < chars.len() {
        match chars[i] {
            '.' => {
                i += 1;
                if i < chars.len() && chars[i] == '.' {
                    tokens.push(Token::RecursiveDescent);
                    i += 1;
                }
                if i >= chars.len() {
                    break;
                }
                if chars[i] == '*' {
                    tokens.push(Token::Wildcard);
                    i += 1;
                } else if chars[i] == '[' {
                    continue;
                } else {
                    let start = i;
                    while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                        i += 1;
                    }
                    let field: String = chars[start..i].iter().collect();
                    if !field.is_empty() {
                        tokens.push(Token::Field(field));
                    }
                }
            }
            '[' => {
                i += 1;
                let start = i;
                let mut depth = 1;
                while i < chars.len() && depth > 0 {
                    if chars[i] == '[' {
                        depth += 1;
                    }
                    if chars[i] == ']' {
                        depth -= 1;
                    }
                    if depth > 0 {
                        i += 1;
                    }
                }
                let inner: String = chars[start..i].iter().collect();
                i += 1; // skip ']'
                let inner = inner.trim();

                if inner == "*" {
                    tokens.push(Token::Wildcard);
                } else if let Some(filter_body) = inner.strip_prefix('?') {
                    tokens.push(Token::Filter(filter_body.trim().to_string()));
                } else if inner.contains(':') {
                    let parts: Vec<&str> = inner.splitn(2, ':').collect();
                    let start = parts[0].trim().parse::<i64>().ok();
                    let end = parts.get(1).and_then(|s| s.trim().parse::<i64>().ok());
                    tokens.push(Token::Slice(start, end));
                } else if let Ok(idx) = inner.parse::<i64>() {
                    tokens.push(Token::Index(idx));
                } else {
                    let field = inner.trim_matches(|c| c == '\'' || c == '"');
                    tokens.push(Token::Field(field.to_string()));
                }
            }
            _ => {
                let start = i;
                while i < chars.len() && chars[i] != '.' && chars[i] != '[' {
                    i += 1;
                }
                let field: String = chars[start..i].iter().collect();
                if !field.is_empty() {
                    tokens.push(Token::Field(field));
                }
            }
        }
    }

    Ok(tokens)
}

fn evaluate(value: &Value, tokens: &[Token]) -> Vec<Value> {
    if tokens.is_empty() {
        return vec![value.clone()];
    }

    let mut current = vec![value.clone()];

    for token in tokens {
        let mut next = Vec::new();
        for val in &current {
            match token {
                Token::Root => {
                    next.push(val.clone());
                }
                Token::Field(key) => {
                    if let Value::Object(map) = val {
                        if let Some(v) = map.get(key) {
                            next.push(v.clone());
                        }
                    }
                }
                Token::Index(idx) => {
                    if let Value::Array(arr) = val {
                        let real_idx = if *idx < 0 {
                            (arr.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if let Some(v) = arr.get(real_idx) {
                            next.push(v.clone());
                        }
                    }
                }
                Token::Wildcard => match val {
                    Value::Array(arr) => {
                        next.extend(arr.iter().cloned());
                    }
                    Value::Object(map) => {
                        next.extend(map.values().cloned());
                    }
                    _ => {}
                },
                Token::Slice(start, end) => {
                    if let Value::Array(arr) = val {
                        let len = arr.len() as i64;
                        let s = start.unwrap_or(0);
                        let e = end.unwrap_or(len);
                        let s = if s < 0 {
                            (len + s).max(0) as usize
                        } else {
                            s as usize
                        };
                        let e = if e < 0 {
                            (len + e).max(0) as usize
                        } else {
                            (e as usize).min(arr.len())
                        };
                        if s < e {
                            next.extend(arr[s..e].iter().cloned());
                        }
                    }
                }
                Token::RecursiveDescent => {
                    collect_recursive(val, &mut next);
                }
                Token::Filter(expr) => {
                    if let Value::Array(arr) = val {
                        for item in arr {
                            if eval_filter(item, expr) {
                                next.push(item.clone());
                            }
                        }
                    }
                }
            }
        }
        current = next;
    }

    current
}

fn collect_recursive(val: &Value, out: &mut Vec<Value>) {
    out.push(val.clone());
    match val {
        Value::Array(arr) => {
            for item in arr {
                collect_recursive(item, out);
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                collect_recursive(v, out);
            }
        }
        _ => {}
    }
}

fn eval_filter(item: &Value, expr: &str) -> bool {
    let expr = expr.trim().trim_start_matches('(').trim_end_matches(')');

    for op in [">=", "<=", "!=", "==", ">", "<"] {
        if let Some(pos) = expr.find(op) {
            let left = expr[..pos]
                .trim()
                .trim_start_matches('@')
                .trim_start_matches('.');
            let right = expr[pos + op.len()..].trim();

            let left_val = if left.is_empty() {
                item.clone()
            } else {
                item.get(left).cloned().unwrap_or(Value::Null)
            };

            let right_val: Value = serde_json::from_str(right).unwrap_or(Value::String(
                right.trim_matches(|c| c == '\'' || c == '"').to_string(),
            ));

            return compare_values(&left_val, &right_val, op);
        }
    }

    let field = expr.trim_start_matches('@').trim_start_matches('.');
    if field.is_empty() {
        !item.is_null()
    } else {
        item.get(field).is_some_and(|v| !v.is_null())
    }
}

fn compare_values(left: &Value, right: &Value, op: &str) -> bool {
    match (to_f64(left), to_f64(right)) {
        (Some(l), Some(r)) => match op {
            "==" => (l - r).abs() < f64::EPSILON,
            "!=" => (l - r).abs() >= f64::EPSILON,
            ">" => l > r,
            ">=" => l >= r,
            "<" => l < r,
            "<=" => l <= r,
            _ => false,
        },
        _ => {
            let l = value_to_str(left);
            let r = value_to_str(right);
            match op {
                "==" => l == r,
                "!=" => l != r,
                ">" => l > r,
                ">=" => l >= r,
                "<" => l < r,
                "<=" => l <= r,
                _ => false,
            }
        }
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn value_to_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub fn syntax_guide() -> &'static str {
    r#"
  JSONPath Syntax Guide
  =====================

  Expression          Description                          Example
  ──────────────────────────────────────────────────────────────────────────
  $                   Root element                         $
  $.key               Object field                         $.name
  $['key']            Object field (bracket)               $['first name']
  $.a.b.c             Nested field                         $.address.city
  $[0]                Array element by index               $[0]
  $[-1]               Last array element                   $[-1]
  $[0:5]              Array slice [start:end)              $[0:5]
  $[*]                All elements (wildcard)              $[*]
  $[*].key            Field from each element              $[*].name
  $..*                Recursive descent (all nodes)        $..*
  $[?(@.key > val)]   Filter by condition                  $[?(@.age > 30)]

  Filter Operators
  ──────────────────────────────────────────────────────────────────────────
  ==    Equal                $[?(@.status == "active")]
  !=    Not equal            $[?(@.role != "admin")]
  >     Greater than         $[?(@.age > 18)]
  >=    Greater or equal     $[?(@.score >= 90)]
  <     Less than            $[?(@.price < 100)]
  <=    Less or equal        $[?(@.count <= 5)]

  Quick Examples
  ──────────────────────────────────────────────────────────────────────────

  # Given: [{"name":"Alice","age":30}, {"name":"Bob","age":25}]

  $[*].name            → ["Alice", "Bob"]
  $[0]                 → {"name":"Alice","age":30}
  $[?(@.age > 28)]     → [{"name":"Alice","age":30}]
  $[-1].name           → "Bob"
  $[0:1]               → [{"name":"Alice","age":30}]
"#
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!([
            {"name": "Alice", "age": 30, "city": "Beijing"},
            {"name": "Bob", "age": 25, "city": "Shanghai"},
            {"name": "Charlie", "age": 35, "city": "Beijing"}
        ])
    }

    #[test]
    fn test_root() {
        let data = sample();
        let r = parse_and_eval(&data, "$").unwrap();
        assert_eq!(r.len(), 1);
        assert!(r[0].is_array());
    }

    #[test]
    fn test_wildcard_field() {
        let data = sample();
        let r = parse_and_eval(&data, "$[*].name").unwrap();
        assert_eq!(r, vec![json!("Alice"), json!("Bob"), json!("Charlie")]);
    }

    #[test]
    fn test_index() {
        let data = sample();
        let r = parse_and_eval(&data, "$[0]").unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0]["name"], json!("Alice"));
    }

    #[test]
    fn test_negative_index() {
        let data = sample();
        let r = parse_and_eval(&data, "$[-1].name").unwrap();
        assert_eq!(r, vec![json!("Charlie")]);
    }

    #[test]
    fn test_slice() {
        let data = sample();
        let r = parse_and_eval(&data, "$[0:2]").unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0]["name"], json!("Alice"));
        assert_eq!(r[1]["name"], json!("Bob"));
    }

    #[test]
    fn test_nested_field() {
        let data = json!({"user": {"profile": {"name": "Alice"}}});
        let r = parse_and_eval(&data, "$.user.profile.name").unwrap();
        assert_eq!(r, vec![json!("Alice")]);
    }

    #[test]
    fn test_filter_gt() {
        let data = sample();
        let r = parse_and_eval(&data, "$[?(@.age > 28)]").unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_filter_eq_string() {
        let data = sample();
        let r = parse_and_eval(&data, "$[?(@.city == \"Beijing\")]").unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_bracket_field() {
        let data = json!({"first name": "Alice"});
        let r = parse_and_eval(&data, "$['first name']").unwrap();
        assert_eq!(r, vec![json!("Alice")]);
    }

    #[test]
    fn test_list_keys_object() {
        let data = json!({"a": 1, "b": 2, "c": 3});
        let keys = list_keys(&data, None).unwrap();
        assert!(keys.contains(&"a".to_string()));
        assert!(keys.contains(&"b".to_string()));
    }

    #[test]
    fn test_list_keys_array() {
        let data = sample();
        let keys = list_keys(&data, None).unwrap();
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
    }

    #[test]
    fn test_wildcard_object_values() {
        let data = json!({"a": 1, "b": 2, "c": 3});
        let r = parse_and_eval(&data, "$.*").unwrap();
        assert_eq!(r.len(), 3);
    }
}
