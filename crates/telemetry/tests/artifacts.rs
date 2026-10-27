//! Lint of the importable artifacts in `deploy/telemetry/` (TE §7, TE-T7):
//! the Grafana dashboard JSON against the structure Grafana's schema
//! version 39 expects, and every PromQL query of the dashboards and rule
//! files against the name catalog in its Prometheus 3 / Mimir OTLP spelling.
//! `promtool check rules` and `promtool test rules`
//! (`scripts/check-telemetry-artifacts.sh`) check the PromQL syntax and the
//! alert behaviour; this file checks that the names exist.
use merezha_telemetry::{
    artifacts::{self, ARTIFACTS},
    names::{self, Instrument, Metric},
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

fn deploy() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/telemetry")
}

// ---------------------------------------------------------------------------
// The OTLP to Prometheus translation (UnderscoreEscapingWithSuffixes)
// ---------------------------------------------------------------------------

/// The series names Prometheus 3 and Mimir give a catalog metric: dots
/// become underscores, the unit adds `_seconds`, `_bytes` or (for gauges of
/// unit `1`) `_ratio` unless the name already holds it, counters end in
/// `_total`, histograms expose `_bucket`, `_count` and `_sum`.
fn series_names(metric: &Metric) -> Vec<String> {
    let mut tokens: Vec<String> = metric.name.split(['.', '-']).map(str::to_owned).collect();
    let unit = match metric.unit {
        "s" => Some("seconds"),
        "By" => Some("bytes"),
        "1" if metric.instrument == Instrument::Gauge => Some("ratio"),
        "1" => None,
        unit if unit.starts_with('{') && unit.ends_with('}') => None,
        unit => panic!(
            "{}: unit {unit} has no translation in this test",
            metric.name
        ),
    };
    if let Some(unit) = unit
        && !tokens.iter().any(|t| t == unit)
    {
        tokens.push(unit.to_owned());
    }
    if metric.instrument == Instrument::Counter {
        tokens.retain(|t| t != "total");
        tokens.push("total".to_owned());
    }
    let base = tokens.join("_");
    match metric.instrument {
        Instrument::Histogram => ["bucket", "count", "sum"]
            .iter()
            .map(|suffix| format!("{base}_{suffix}"))
            .collect(),
        _ => vec![base],
    }
}

fn label(attribute: &str) -> String {
    attribute.replace('.', "_")
}

/// Prometheus series name to (catalog row, labels allowed on it).
fn series_catalog() -> BTreeMap<String, (&'static Metric, BTreeSet<String>)> {
    let mut map = BTreeMap::new();
    for metric in names::METRICS {
        for name in series_names(metric) {
            let mut labels: BTreeSet<String> = metric.attributes.iter().map(|a| label(a)).collect();
            // `service.name` and `service.instance.id`.
            labels.extend(["job".to_owned(), "instance".to_owned()]);
            if name.ends_with("_bucket") {
                labels.insert("le".to_owned());
            }
            assert!(
                map.insert(name.clone(), (metric, labels)).is_none(),
                "two catalog metrics translate to {name}"
            );
        }
    }
    map
}

#[test]
fn the_translation_matches_the_names_prometheus_stored() {
    // Seen in a Prometheus 3.15 fed by `merezha run` through
    // grafana/otel-lgtm 0.38.0 (TE-T7).
    let catalog = series_catalog();
    for name in [
        "http_server_request_duration_seconds_bucket",
        "http_server_request_body_size_bytes_count",
        "http_server_active_requests",
        "db_client_connection_wait_time_seconds_sum",
        "db_client_connection_count",
        "merezha_guest_fuel_consumed_bucket",
        "merezha_guest_fuel_utilization_bucket",
        "merezha_guest_memory_peak_bytes_bucket",
        "merezha_projection_warm_ratio",
        "merezha_projection_lag_age_seconds",
        "merezha_events_appended_total",
        "merezha_outbox_lag_seconds",
        "merezha_telemetry_exported_total",
    ] {
        assert!(catalog.contains_key(name), "{name}");
    }
    assert!(catalog.contains_key("merezha_reactor_halted_ratio"));
    assert!(catalog.contains_key("merezha_content_upload_bytes_bucket"));
    assert!(!catalog.contains_key("merezha_reactor_halted"));
}

// ---------------------------------------------------------------------------
// A PromQL scanner: enough to find selectors, matchers and grouping labels
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Selector {
    metric: Option<String>,
    /// (label, operator, value)
    matchers: Vec<(String, String, String)>,
}

#[derive(Debug, Default)]
struct Scan {
    selectors: Vec<Selector>,
    grouping: Vec<String>,
}

const GROUPING: [&str; 6] = [
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
];
const KEYWORDS: [&str; 7] = ["and", "or", "unless", "bool", "offset", "inf", "nan"];

fn scan(expr: &str) -> Result<Scan, String> {
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    let mut out = Scan::default();
    let skip_ws = |i: &mut usize| {
        while *i < chars.len() && chars[*i].is_whitespace() {
            *i += 1;
        }
    };
    let ident = |i: &mut usize| -> String {
        let start = *i;
        while *i < chars.len()
            && (chars[*i].is_ascii_alphanumeric() || chars[*i] == '_' || chars[*i] == ':')
        {
            *i += 1;
        }
        chars[start..*i].iter().collect()
    };
    let string = |i: &mut usize| -> Result<String, String> {
        let quote = chars[*i];
        *i += 1;
        let mut value = String::new();
        while *i < chars.len() && chars[*i] != quote {
            if chars[*i] == '\\' && quote != '`' {
                *i += 1;
            }
            if let Some(c) = chars.get(*i) {
                value.push(*c);
            }
            *i += 1;
        }
        if *i >= chars.len() {
            return Err(format!("unterminated string in {expr}"));
        }
        *i += 1;
        Ok(value)
    };
    let matchers = |i: &mut usize, selector: &mut Selector| -> Result<(), String> {
        *i += 1; // '{'
        loop {
            skip_ws(i);
            match chars.get(*i) {
                Some('}') => {
                    *i += 1;
                    return Ok(());
                }
                Some(',') => *i += 1,
                Some(c) if c.is_ascii_alphabetic() || *c == '_' => {
                    let name = ident(i);
                    skip_ws(i);
                    let mut op = String::new();
                    while let Some(c @ ('=' | '!' | '~')) = chars.get(*i) {
                        op.push(*c);
                        *i += 1;
                    }
                    if !["=", "!=", "=~", "!~"].contains(&op.as_str()) {
                        return Err(format!("bad matcher operator {op:?} in {expr}"));
                    }
                    skip_ws(i);
                    if !matches!(chars.get(*i), Some('"' | '\'' | '`')) {
                        return Err(format!("matcher {name} without a string in {expr}"));
                    }
                    let value = string(i)?;
                    selector.matchers.push((name, op, value));
                }
                _ => return Err(format!("bad matcher list in {expr}")),
            }
        }
    };
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if matches!(c, '"' | '\'' | '`') {
            string(&mut i)?;
        } else if c == '[' {
            while i < chars.len() && chars[i] != ']' {
                i += 1;
            }
            i += 1;
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '.') {
                i += 1;
            }
        } else if c.is_ascii_alphabetic() || c == '_' || c == ':' {
            let word = ident(&mut i);
            let mut j = i;
            skip_ws(&mut j);
            let next = chars.get(j).copied();
            if GROUPING.contains(&word.as_str()) {
                if next == Some('(') {
                    i = j + 1;
                    loop {
                        skip_ws(&mut i);
                        match chars.get(i) {
                            Some(')') => {
                                i += 1;
                                break;
                            }
                            Some(',') => i += 1,
                            Some(_) => {
                                let name = ident(&mut i);
                                if name.is_empty() {
                                    return Err(format!("bad grouping list in {expr}"));
                                }
                                out.grouping.push(name);
                            }
                            None => return Err(format!("unterminated grouping in {expr}")),
                        }
                    }
                }
            } else if next == Some('(')
                || KEYWORDS.contains(&word.as_str())
                || ["by", "without"].iter().any(|g| {
                    let rest: String = chars[j..].iter().take(g.len() + 1).collect();
                    rest.strip_prefix(g)
                        .is_some_and(|r| r.is_empty() || r.starts_with([' ', '(']))
                })
            {
                // A function, an aggregation (`sum by (…) (…)`) or a keyword.
            } else {
                let mut selector = Selector {
                    metric: Some(word),
                    ..Selector::default()
                };
                if next == Some('{') {
                    i = j;
                    matchers(&mut i, &mut selector)?;
                }
                out.selectors.push(selector);
            }
        } else if c == '{' {
            let mut selector = Selector::default();
            matchers(&mut i, &mut selector)?;
            out.selectors.push(selector);
        } else if "()+-*/%^<>=!,".contains(c) {
            i += 1;
        } else {
            return Err(format!("unexpected {c:?} in {expr}"));
        }
    }
    Ok(out)
}

#[test]
fn the_scanner_finds_selectors_matchers_and_grouping() {
    let s = scan(
        r#"histogram_quantile(0.95, sum by (http_route, le) (rate(http_server_request_duration_seconds_bucket{job=~"$service", http_route!~"/health.*"}[$__rate_interval]))) / 1e-3 > bool 2 and on (job) count({otel_metric_overflow="true"}) or vector(0)"#,
    )
    .unwrap();
    assert_eq!(s.grouping, ["http_route", "le", "job"]);
    assert_eq!(s.selectors.len(), 2);
    assert_eq!(
        s.selectors[0].metric.as_deref(),
        Some("http_server_request_duration_seconds_bucket")
    );
    assert_eq!(
        s.selectors[0].matchers[1],
        ("http_route".into(), "!~".into(), "/health.*".into())
    );
    assert_eq!(s.selectors[1].metric, None);
    assert!(scan("rate(x[5m]) $oops").is_err());
    assert!(scan(r#"x{job="a""#).is_err());
}

// ---------------------------------------------------------------------------
// Rule files (Prometheus rule-file YAML, read line by line)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Rule {
    record: Option<String>,
    alert: Option<String>,
    expr: String,
    severity: Option<String>,
    annotations: BTreeSet<String>,
    has_for: bool,
}

/// The rules of a file written in this directory's style: `- record:` or
/// `- alert:` opens a rule; `expr:` is inline or a `|` block.
fn rules(text: &str) -> Vec<Rule> {
    let mut rules: Vec<Rule> = Vec::new();
    let mut lines = text.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        if let Some(name) = trimmed.strip_prefix("- record:") {
            rules.push(Rule {
                record: Some(name.trim().to_owned()),
                ..Rule::default()
            });
        } else if let Some(name) = trimmed.strip_prefix("- alert:") {
            rules.push(Rule {
                alert: Some(name.trim().to_owned()),
                ..Rule::default()
            });
        } else if let Some(rule) = rules.last_mut() {
            if let Some(expr) = trimmed.strip_prefix("expr:") {
                let expr = expr.trim();
                if expr == "|" {
                    while let Some(next) = lines.peek() {
                        let next_indent = next.len() - next.trim_start().len();
                        if next.trim().is_empty() || next_indent <= indent {
                            break;
                        }
                        rule.expr.push_str(next.trim());
                        rule.expr.push('\n');
                        lines.next();
                    }
                } else {
                    rule.expr = expr.to_owned();
                }
            } else if let Some(severity) = trimmed.strip_prefix("severity:") {
                rule.severity = Some(severity.trim().to_owned());
            } else if trimmed.starts_with("for:") {
                rule.has_for = true;
            } else if let Some((key, _)) = trimmed.split_once(':')
                && ["summary", "description"].contains(&key)
            {
                rule.annotations.insert(key.to_owned());
            }
        }
    }
    rules
}

fn rule_files() -> Vec<(&'static str, Vec<Rule>)> {
    ARTIFACTS
        .iter()
        .filter(|a| a.path.starts_with("rules/"))
        .map(|a| (a.path, rules(a.contents)))
        .collect()
}

// ---------------------------------------------------------------------------
// Name checks shared by dashboards and rules
// ---------------------------------------------------------------------------

/// The names known to queries: catalog series and recording rules, each with
/// the labels it may carry.
struct Names {
    series: BTreeMap<String, (&'static Metric, BTreeSet<String>)>,
    recorded: BTreeMap<String, BTreeSet<String>>,
}

/// Labels any query may use: the resource labels and the overflow marker.
const UNIVERSAL: [&str; 4] = ["job", "instance", "__name__", "otel_metric_overflow"];

impl Names {
    fn new() -> Self {
        let mut names = Self {
            series: series_catalog(),
            recorded: BTreeMap::new(),
        };
        for (path, rules) in rule_files() {
            for rule in rules {
                if let Some(record) = &rule.record {
                    let labels = names
                        .check(&rule.expr, false)
                        .unwrap_or_else(|e| panic!("{path} {record}: {e}"));
                    names.recorded.insert(record.clone(), labels);
                }
            }
        }
        names
    }

    /// Check one query; returns the labels its selectors may carry.
    /// `dashboard`: every selector must filter `job=~"$service"`.
    fn check(&self, expr: &str, dashboard: bool) -> Result<BTreeSet<String>, String> {
        let scan = scan(expr)?;
        if scan.selectors.is_empty() {
            return Err(format!("no selector in {expr}"));
        }
        let mut allowed: BTreeSet<String> = UNIVERSAL.iter().map(|l| (*l).to_owned()).collect();
        for selector in &scan.selectors {
            let labels = match &selector.metric {
                Some(name) => match (self.series.get(name), self.recorded.get(name)) {
                    (Some((_, labels)), _) | (None, Some(labels)) => labels.clone(),
                    (None, None) => {
                        return Err(format!(
                            "{name} is neither a catalog series nor a recording rule"
                        ));
                    }
                },
                None => ["job", "otel_metric_overflow"]
                    .iter()
                    .map(|l| (*l).to_owned())
                    .collect(),
            };
            for (label, _, _) in &selector.matchers {
                if !labels.contains(label) {
                    return Err(format!(
                        "label {label} is not an attribute of {:?}",
                        selector.metric
                    ));
                }
            }
            if dashboard
                && !selector
                    .matchers
                    .iter()
                    .any(|m| m == &("job".to_owned(), "=~".to_owned(), "$service".to_owned()))
            {
                return Err(format!(
                    "selector {:?} does not filter job=~\"$service\" in {expr}",
                    selector.metric
                ));
            }
            allowed.extend(labels);
        }
        for label in &scan.grouping {
            if !allowed.contains(label) {
                return Err(format!(
                    "grouping label {label} is not on the selected series of {expr}"
                ));
            }
        }
        Ok(allowed)
    }
}

// ---------------------------------------------------------------------------
// Dashboards
// ---------------------------------------------------------------------------

fn dashboards() -> Vec<(&'static str, Value)> {
    ARTIFACTS
        .iter()
        .filter(|a| a.path.starts_with("dashboards/"))
        .map(|a| {
            (
                a.path,
                serde_json::from_str(a.contents).unwrap_or_else(|e| panic!("{}: {e}", a.path)),
            )
        })
        .collect()
}

/// Every Prometheus query of a dashboard: (panel title, expr).
fn prometheus_queries(dashboard: &Value) -> Vec<(String, String)> {
    let mut queries = Vec::new();
    for panel in dashboard["panels"].as_array().into_iter().flatten() {
        for target in panel["targets"].as_array().into_iter().flatten() {
            if target["datasource"]["type"] == "prometheus" {
                queries.push((
                    panel["title"].as_str().unwrap_or_default().to_owned(),
                    target["expr"].as_str().unwrap_or_default().to_owned(),
                ));
            }
        }
    }
    queries
}

/// Every `datasource` object anywhere in the document.
fn datasources<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if key == "datasource" {
                    out.push(v);
                }
                datasources(v, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|v| datasources(v, out)),
        _ => {}
    }
}

const PANEL_TYPES: [&str; 6] = ["timeseries", "stat", "heatmap", "logs", "table", "row"];
const DATASOURCE_VARIABLES: [(&str, &str); 3] = [
    ("DS_PROMETHEUS", "prometheus"),
    ("DS_LOKI", "loki"),
    ("DS_TEMPO", "tempo"),
];

/// The structure Grafana requires to import or provision a dashboard of
/// schema version 39, plus this project's conventions (TE §7).
fn lint_dashboard(path: &str, d: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    let mut fail = |p: String| problems.push(format!("{path}: {p}"));
    let uid = d["uid"].as_str().unwrap_or_default();
    if !uid.starts_with("merezha-")
        || uid.len() > 40
        || !uid
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        fail(format!("uid {uid:?}"));
    }
    if path != format!("dashboards/{uid}.json") {
        fail("file name differs from the uid".into());
    }
    if !d["title"]
        .as_str()
        .is_some_and(|t| t.starts_with("Merezha / "))
    {
        fail("title must start with \"Merezha / \"".into());
    }
    if d["schemaVersion"] != 39 {
        fail("schemaVersion must be 39".into());
    }
    if !d["id"].is_null() {
        fail("a dashboard id would collide on import; leave it out".into());
    }
    if d.get("__inputs").is_some() {
        fail("__inputs is not resolved by file provisioning; use the DS_* variables".into());
    }
    if !d["tags"]
        .as_array()
        .is_some_and(|t| t.iter().any(|t| t == "merezha"))
    {
        fail("tag merezha missing".into());
    }
    for key in ["time", "templating", "annotations"] {
        if !d[key].is_object() {
            fail(format!("{key} must be an object"));
        }
    }
    // Template variables.
    let variables = d["templating"]["list"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut declared = BTreeMap::new();
    for v in &variables {
        let name = v["name"].as_str().unwrap_or_default();
        if declared.insert(name.to_owned(), v.clone()).is_some() {
            fail(format!("variable {name} twice"));
        }
        if v["type"] == "datasource" {
            match DATASOURCE_VARIABLES.iter().find(|(n, _)| *n == name) {
                Some((_, kind)) if v["query"] == *kind => {}
                _ => fail(format!(
                    "datasource variable {name} must be DS_<TYPE> of its type"
                )),
            }
        }
    }
    match declared.get("service") {
        Some(v) => {
            if v["type"] != "query"
                || v["datasource"]["uid"] != "${DS_PROMETHEUS}"
                || v["includeAll"] != true
                || v["allValue"] != ".+"
            {
                fail("variable service must be a Prometheus query with All = .+".into());
            }
            let definition = v["definition"].as_str().unwrap_or_default();
            let metric = definition
                .strip_prefix("label_values(")
                .and_then(|r| r.strip_suffix(", job)"))
                .unwrap_or_default();
            if !series_catalog().contains_key(metric) {
                fail(format!("variable service reads {definition}"));
            }
        }
        None => fail("variable service missing".into()),
    }
    // Datasource references: always a declared DS_* variable of its type.
    let mut refs = Vec::new();
    datasources(d, &mut refs);
    for r in refs {
        let uid = r["uid"].as_str().unwrap_or_default();
        let kind = r["type"].as_str().unwrap_or_default();
        let ok = DATASOURCE_VARIABLES.iter().any(|(name, k)| {
            *k == kind && uid == format!("${{{name}}}") && declared.contains_key(*name)
        });
        if !ok {
            fail(format!(
                "datasource {r} is not a declared ${{DS_*}} variable"
            ));
        }
    }
    // Panels.
    let panels = d["panels"].as_array().cloned().unwrap_or_default();
    if panels.is_empty() {
        fail("no panels".into());
    }
    let mut ids = BTreeSet::new();
    let mut titles = BTreeSet::new();
    let mut cells: BTreeSet<(i64, i64)> = BTreeSet::new();
    for p in &panels {
        let title = p["title"].as_str().unwrap_or_default();
        let kind = p["type"].as_str().unwrap_or_default();
        if title.is_empty() || !titles.insert(title.to_owned()) {
            fail(format!("panel title {title:?} empty or repeated"));
        }
        if !PANEL_TYPES.contains(&kind) {
            fail(format!("{title}: panel type {kind}"));
        }
        match p["id"].as_i64() {
            Some(id) if id > 0 && ids.insert(id) => {}
            _ => fail(format!("{title}: id missing or repeated")),
        }
        let g = &p["gridPos"];
        let (x, y, w, h) = (
            g["x"].as_i64().unwrap_or(-1),
            g["y"].as_i64().unwrap_or(-1),
            g["w"].as_i64().unwrap_or(0),
            g["h"].as_i64().unwrap_or(0),
        );
        if x < 0 || y < 0 || w < 1 || h < 1 || x + w > 24 {
            fail(format!("{title}: gridPos {g}"));
        }
        for cx in x..x + w {
            for cy in y..y + h {
                if !cells.insert((cx, cy)) {
                    fail(format!("{title}: overlaps another panel"));
                }
            }
        }
        if kind == "row" {
            if p["collapsed"] != false || w != 24 || h != 1 {
                fail(format!("{title}: rows are expanded and full width"));
            }
            continue;
        }
        let targets = p["targets"].as_array().cloned().unwrap_or_default();
        if targets.is_empty() {
            fail(format!("{title}: no target"));
        }
        let mut ref_ids = BTreeSet::new();
        for t in &targets {
            if t["datasource"] != p["datasource"] {
                fail(format!(
                    "{title}: target datasource differs from the panel's"
                ));
            }
            if !t["refId"]
                .as_str()
                .is_some_and(|r| ref_ids.insert(r.to_owned()))
            {
                fail(format!("{title}: refId missing or repeated"));
            }
            match p["datasource"]["type"].as_str() {
                Some("prometheus") => {
                    if !t["expr"].as_str().is_some_and(|e| !e.is_empty()) {
                        fail(format!("{title}: empty expr"));
                    }
                }
                Some("loki") => {
                    if !t["expr"]
                        .as_str()
                        .is_some_and(|e| e.starts_with(r#"{service_name=~"$service"}"#))
                    {
                        fail(format!(
                            "{title}: Loki queries select service_name=~\"$service\""
                        ));
                    }
                }
                Some("tempo") => {
                    if !t["query"]
                        .as_str()
                        .is_some_and(|q| q.contains(r#"resource.service.name=~"$service""#))
                    {
                        fail(format!(
                            "{title}: Tempo queries select resource.service.name=~\"$service\""
                        ));
                    }
                }
                other => fail(format!("{title}: datasource type {other:?}")),
            }
        }
        if kind == "heatmap" && targets.iter().any(|t| t["format"] != "heatmap") {
            fail(format!("{title}: heatmap targets use format heatmap"));
        }
    }
    problems
}

#[test]
fn dashboards_pass_the_schema_lint() {
    let dashboards = dashboards();
    let mut problems = Vec::new();
    for (path, d) in &dashboards {
        problems.extend(lint_dashboard(path, d));
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    let titles: BTreeSet<&str> = dashboards
        .iter()
        .map(|(_, d)| d["title"].as_str().unwrap())
        .collect();
    assert_eq!(
        titles,
        BTreeSet::from([
            "Merezha / Overview",
            "Merezha / Application",
            "Merezha / Data and jobs",
            "Merezha / Delivery, identity and content",
            "Merezha / Telemetry health",
        ]),
        "the five dashboards of TE §7"
    );
}

#[test]
fn the_lint_refuses_broken_dashboards() {
    let (path, good) = dashboards().remove(0);
    assert!(lint_dashboard(path, &good).is_empty());
    let broken = |edit: &dyn Fn(&mut Value)| {
        let mut d = good.clone();
        edit(&mut d);
        lint_dashboard(path, &d)
    };
    assert!(!broken(&|d| d["schemaVersion"] = 41.into()).is_empty());
    assert!(!broken(&|d| d["panels"][1]["datasource"]["uid"] = "prometheus".into()).is_empty());
    assert!(!broken(&|d| d["panels"][1]["id"] = d["panels"][0]["id"].clone()).is_empty());
    assert!(!broken(&|d| d["panels"][1]["gridPos"] = d["panels"][0]["gridPos"].clone()).is_empty());
    assert!(!broken(&|d| d["panels"][0]["type"] = "graph".into()).is_empty());
    assert!(
        !broken(&|d| d["templating"]["list"]
            .as_array_mut()
            .unwrap()
            .retain(|v| v["name"] != "service"))
        .is_empty()
    );
}

#[test]
fn dashboard_queries_name_only_catalog_series_and_labels() {
    let names = Names::new();
    let mut problems = Vec::new();
    for (path, d) in dashboards() {
        for (title, expr) in prometheus_queries(&d) {
            if let Err(e) = names.check(&expr, true) {
                problems.push(format!("{path} {title}: {e}"));
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn the_name_check_refuses_unknown_series_and_labels() {
    let names = Names::new();
    assert!(
        names
            .check(
                r#"merezha_command_duration_seconds_count{job=~"$service"}"#,
                true
            )
            .is_ok()
    );
    for bad in [
        // Not the catalog's spelling.
        r#"merezha_reactor_halted{job=~"$service"}"#,
        r#"merezha_command_duration_count{job=~"$service"}"#,
        // A label of another instrument.
        r#"merezha_command_duration_seconds_count{job=~"$service", merezha_query_tag="x"}"#,
        r#"sum by (job_queue) (merezha_command_duration_seconds_count{job=~"$service"})"#,
        // `le` exists on buckets only.
        r#"sum by (le) (merezha_command_duration_seconds_count{job=~"$service"})"#,
        // Dashboards filter on the service variable.
        "merezha_command_duration_seconds_count",
    ] {
        assert!(names.check(bad, true).is_err(), "{bad}");
    }
}

#[test]
fn every_catalog_metric_is_charted_or_listed_as_uncharted() {
    // Metrics no dashboard shows, with the reason. Empty today: every
    // catalog row has a panel. A new catalog metric needs a panel or a
    // line here.
    const UNCHARTED: &[(&str, &str)] = &[];
    let catalog = series_catalog();
    let mut charted = BTreeSet::new();
    for (_, d) in dashboards() {
        for (_, expr) in prometheus_queries(&d) {
            for selector in scan(&expr).unwrap().selectors {
                if let Some((metric, _)) = selector.metric.and_then(|m| catalog.get(&m)) {
                    charted.insert(metric.name);
                }
            }
        }
    }
    let missing: Vec<&str> = names::METRICS
        .iter()
        .map(|m| m.name)
        .filter(|n| !charted.contains(n) && !UNCHARTED.iter().any(|(u, _)| u == n))
        .collect();
    assert!(missing.is_empty(), "no dashboard panel shows {missing:?}");
}

#[test]
fn the_overview_shows_the_signals_the_example_application_reads() {
    // docs/design/scaffold.md §5.8 lists these on the overview.
    let overview = &dashboards()
        .into_iter()
        .find(|(p, _)| *p == "dashboards/merezha-overview.json")
        .unwrap()
        .1;
    let catalog = series_catalog();
    let shown: BTreeSet<&str> = prometheus_queries(overview)
        .iter()
        .flat_map(|(_, e)| scan(e).unwrap().selectors)
        .filter_map(|s| s.metric.and_then(|m| catalog.get(&m)).map(|(m, _)| m.name))
        .collect();
    for name in [
        "http.server.request.duration",
        "merezha.command.duration",
        "merezha.reactor.effects",
        "merezha.reactor.lag.events",
        "merezha.notification.deliveries",
        "merezha.inbox.sse.connections",
        "merezha.webhook.deliveries",
        "merezha.job.dead_lettered",
    ] {
        assert!(shown.contains(name), "overview lacks {name}");
    }
    let kinds: BTreeSet<&str> = overview["panels"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|p| p["datasource"]["type"].as_str())
        .collect();
    assert_eq!(kinds, BTreeSet::from(["prometheus", "loki", "tempo"]));
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

#[test]
fn rules_name_only_catalog_series_and_labels() {
    let names = Names::new();
    let mut alerts = BTreeSet::new();
    let mut records = BTreeSet::new();
    let mut problems = Vec::new();
    for (path, rules) in rule_files() {
        assert!(!rules.is_empty(), "{path} holds no rule");
        for rule in rules {
            let name = rule.record.clone().or(rule.alert.clone()).unwrap();
            if rule.expr.trim().is_empty() {
                problems.push(format!("{path} {name}: no expr"));
                continue;
            }
            if let Err(e) = names.check(&rule.expr, false) {
                problems.push(format!("{path} {name}: {e}"));
            }
            if let Some(record) = &rule.record {
                let parts: Vec<&str> = record.split(':').collect();
                if parts.len() != 3 || parts[0] != "job" || !parts[1].starts_with("merezha_") {
                    problems.push(format!(
                        "{path} {record}: recording rules are job:merezha_<what>:<operation>"
                    ));
                }
                if !records.insert(record.clone()) {
                    problems.push(format!("{path} {record} twice"));
                }
            }
            if let Some(alert) = &rule.alert {
                if !alert.starts_with("Merezha")
                    || !alert.chars().all(|c| c.is_ascii_alphanumeric())
                {
                    problems.push(format!("{path} {alert}: alerts are MerezhaCamelCase"));
                }
                if !alerts.insert(alert.clone()) {
                    problems.push(format!("{path} {alert} twice"));
                }
                if !matches!(rule.severity.as_deref(), Some("page" | "ticket")) {
                    problems.push(format!("{path} {alert}: severity page or ticket"));
                }
                if rule.annotations.len() != 2 {
                    problems.push(format!("{path} {alert}: summary and description"));
                }
                let _ = rule.has_for;
            }
        }
    }
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    // TE §7's table: every SLO and alert row has a rule.
    for alert in [
        "MerezhaHttpAvailabilityBudgetBurn",
        "MerezhaHttpAvailabilityBudgetBurnSlow",
        "MerezhaHttpLatencyBudgetBurn",
        "MerezhaHttpLatencyBudgetBurnSlow",
        "MerezhaCommandFailures",
        "MerezhaCommandRejections",
        "MerezhaProjectionLagging",
        "MerezhaProjectionStalled",
        "MerezhaReactorHalted",
        "MerezhaOutboxLagging",
        "MerezhaJobsLagging",
        "MerezhaJobsDeadLettered",
        "MerezhaNotificationDeliveryFailures",
        "MerezhaNotificationDeliveryFailuresHigh",
        "MerezhaDatabasePoolSaturated",
        "MerezhaDatabasePoolTimeouts",
        "MerezhaGuestLimits",
        "MerezhaInstanceAbsent",
        "MerezhaTelemetryDropped",
        "MerezhaMetricOverflow",
    ] {
        assert!(alerts.contains(alert), "{alert} missing");
    }
}

#[test]
fn the_rule_reader_sees_block_and_inline_expressions() {
    let rules = rules(
        "groups:\n  - name: g\n    rules:\n      - record: job:merezha_x:ratio\n        expr: |\n          sum by (job) (a)\n          /\n          sum by (job) (b)\n      - alert: MerezhaX\n        expr: job:merezha_x:ratio > 1\n        for: 5m\n        labels:\n          severity: page\n        annotations:\n          summary: 's'\n          description: 'd'\n",
    );
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].expr, "sum by (job) (a)\n/\nsum by (job) (b)\n");
    assert_eq!(rules[1].expr, "job:merezha_x:ratio > 1");
    assert_eq!(rules[1].severity.as_deref(), Some("page"));
    assert!(rules[1].has_for);
    assert_eq!(rules[1].annotations.len(), 2);
}

// ---------------------------------------------------------------------------
// The embedded set and the provisioning file
// ---------------------------------------------------------------------------

#[test]
fn the_embedded_set_is_every_file_of_deploy_telemetry() {
    let mut on_disk = BTreeSet::from(["dashboards.yaml".to_owned()]);
    for dir in ["dashboards", "rules"] {
        for entry in fs::read_dir(deploy().join(dir)).unwrap() {
            let name = entry.unwrap().file_name().into_string().unwrap();
            on_disk.insert(format!("{dir}/{name}"));
        }
    }
    let embedded: BTreeSet<String> = ARTIFACTS.iter().map(|a| a.path.to_owned()).collect();
    assert_eq!(embedded, on_disk);
    for artifact in ARTIFACTS {
        assert_eq!(
            fs::read_to_string(deploy().join(artifact.path)).unwrap(),
            artifact.contents
        );
    }
}

#[test]
fn provisioning_reads_the_directory_the_lgtm_image_gets() {
    // TE §6: ops/telemetry/lgtm.Dockerfile copies dashboards/ there.
    let provisioning = ARTIFACTS
        .iter()
        .find(|a| a.path == "dashboards.yaml")
        .unwrap()
        .contents;
    assert!(provisioning.contains("apiVersion: 1"));
    assert!(provisioning.contains("type: file"));
    assert!(
        provisioning.contains("path: /otel-lgtm/grafana/conf/provisioning/dashboards/merezha\n")
    );
}

#[test]
fn export_writes_every_file_and_replaces_its_own() {
    let dir = std::env::temp_dir().join(format!("merezha-artifacts-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("dashboards")).unwrap();
    fs::write(dir.join("lgtm.Dockerfile"), "FROM x\n").unwrap();
    fs::write(dir.join("dashboards/merezha-overview.json"), "stale").unwrap();
    let written = artifacts::export(&dir).unwrap();
    assert_eq!(
        written,
        ARTIFACTS.iter().map(|a| a.path).collect::<Vec<_>>()
    );
    for artifact in ARTIFACTS {
        assert_eq!(
            fs::read_to_string(dir.join(artifact.path)).unwrap(),
            artifact.contents
        );
    }
    assert_eq!(
        fs::read_to_string(dir.join("lgtm.Dockerfile")).unwrap(),
        "FROM x\n"
    );
    // No temporary file is left behind.
    let leftovers: Vec<_> = ["", "dashboards", "rules"]
        .iter()
        .flat_map(|d| fs::read_dir(dir.join(d)).unwrap())
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with('.'))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    // A file where a directory must go stops the export and names it.
    let blocked = dir.join("blocked");
    fs::create_dir_all(&blocked).unwrap();
    fs::write(blocked.join("rules"), "not a directory").unwrap();
    let error = artifacts::export(&blocked).unwrap_err();
    assert_eq!(error.path, Some("rules/merezha-recording.yaml"));
    fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[test]
fn export_replaces_a_symbolic_link_instead_of_following_it() {
    let dir = std::env::temp_dir().join(format!("merezha-artifacts-link-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("rules")).unwrap();
    let outside = dir.join("outside.yaml");
    fs::write(&outside, "keep").unwrap();
    std::os::unix::fs::symlink(&outside, dir.join("rules/merezha-alerts.yaml")).unwrap();
    artifacts::export(&dir).unwrap();
    assert_eq!(fs::read_to_string(&outside).unwrap(), "keep");
    assert!(
        !fs::symlink_metadata(dir.join("rules/merezha-alerts.yaml"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    fs::remove_dir_all(&dir).unwrap();
}
