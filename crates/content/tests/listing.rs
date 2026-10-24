//! CT-C3 management list (section 5): keyset cursors under concurrent
//! inserts, random filters and sorts against an in-memory evaluation, and
//! the query errors.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::{Map, Value, json};
use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering as Atomic},
    },
};

const LIST: &str = "/v1/content/manage/entries/article";

fn id_of(reply: &Reply) -> String {
    reply.body["sys"]["id"].as_str().unwrap().to_owned()
}

/// Create `fields` sets concurrently, 8 at a time; the ids in order.
async fn create_all(client: &Client, all: Vec<Value>) -> Vec<String> {
    let mut ids = Vec::new();
    for chunk in all.chunks(8) {
        let mut tasks = Vec::new();
        for fields in chunk {
            let client = client.clone();
            let fields = fields.clone();
            tasks.push(tokio::spawn(async move {
                let reply = client
                    .post(LIST)
                    .json(&json!({ "fields": fields }))
                    .send()
                    .await;
                assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
                id_of(&reply)
            }));
        }
        for task in tasks {
            ids.push(task.await.unwrap());
        }
    }
    ids
}

/// Every page of `query`; the items in order.
async fn walk(client: &Client, query: &str, limit: usize) -> Vec<Value> {
    let mut items = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10_000 {
        let mut uri = format!("{LIST}?limit={limit}");
        if !query.is_empty() {
            uri.push('&');
            uri.push_str(query);
        }
        if let Some(cursor) = &cursor {
            uri.push_str(&format!("&cursor={cursor}"));
        }
        let reply = client.get(&uri).send().await;
        assert_eq!(reply.status, StatusCode::OK, "{uri}: {:?}", reply.body);
        let page = reply.body["items"].as_array().unwrap();
        assert!(page.len() <= limit);
        items.extend(page.iter().cloned());
        match reply.body["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return items,
        }
    }
    panic!("the walk never ended");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_cursor_walk_over_1000_entries_with_concurrent_inserts_has_no_duplicates_or_gaps() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let seeded: Vec<Value> = (0..1000)
        .map(|i| json!({"title": format!("a{i}"), "reading_minutes": i % 7 + 1}))
        .collect();
    let ids: BTreeSet<String> = create_all(&fx.client, seeded).await.into_iter().collect();
    assert_eq!(ids.len(), 1000);
    for (query, limit) in [("sort=reading_minutes", 37), ("", 64), ("sort=-title", 100)] {
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let client = fx.client.clone();
            let stop = stop.clone();
            let query = query.to_owned();
            tokio::spawn(async move {
                let mut n = 0;
                while !stop.load(Atomic::SeqCst) {
                    n += 1;
                    let reply = client
                        .post(LIST)
                        .json(&json!({"fields": {"title": format!("w{query}{n}"),
                                                  "reading_minutes": n % 7 + 1}}))
                        .send()
                        .await;
                    assert_eq!(reply.status, StatusCode::CREATED, "{:?}", reply.body);
                }
                n
            })
        };
        let items = walk(&fx.client, query, limit).await;
        stop.store(true, Atomic::SeqCst);
        let inserted = writer.await.unwrap();
        assert!(inserted > 0);
        let walked: Vec<String> = items
            .iter()
            .map(|i| i["sys"]["id"].as_str().unwrap().to_owned())
            .collect();
        let unique: HashSet<&String> = walked.iter().collect();
        assert_eq!(unique.len(), walked.len(), "{query}: duplicates");
        let missing: Vec<&String> = ids.iter().filter(|id| !unique.contains(id)).collect();
        assert!(missing.is_empty(), "{query}: gaps {missing:?}");
    }
}

/// A small deterministic generator (xorshift64*), so a failure replays.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const LETTERS: [&str; 5] = ["a", "b", "c", "d", "e"];
const CATEGORIES: [&str; 3] = ["news", "guide", "release"];
const TAGS: [&str; 3] = ["x", "y", "z"];

fn word(rng: &mut Rng) -> String {
    (0..=rng.below(3))
        .map(|_| *rng.pick(&LETTERS))
        .collect::<String>()
}

/// One generated article's fields, in their stored form.
fn article(rng: &mut Rng, i: usize, authors: &[String]) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("title".into(), json!(word(rng)));
    fields.insert("slug".into(), json!(format!("s{i}")));
    if rng.chance(80) {
        fields.insert("category".into(), json!(rng.pick(&CATEGORIES)));
    }
    if rng.chance(80) {
        fields.insert("reading_minutes".into(), json!(rng.below(20) + 1));
    }
    if rng.chance(70) {
        let cents = rng.below(2000) as i64 - 1000;
        let sign = if cents < 0 { "-" } else { "" };
        fields.insert(
            "score".into(),
            json!(format!(
                "{sign}{}.{:02}",
                cents.abs() / 100,
                cents.abs() % 100
            )),
        );
    }
    if rng.chance(70) {
        fields.insert("featured".into(), json!(rng.chance(50)));
    }
    if rng.chance(70) {
        fields.insert(
            "published_on".into(),
            json!(format!("2026-0{}-1{}", rng.below(9) + 1, rng.below(10))),
        );
    }
    if rng.chance(70) {
        let tags: Vec<&str> = TAGS.iter().copied().filter(|_| rng.chance(50)).collect();
        fields.insert("tags".into(), json!(tags));
    }
    if rng.chance(60) {
        fields.insert(
            "author".into(),
            json!({"id": rng.pick(authors), "type": "author"}),
        );
    }
    fields
}

/// The in-memory meaning of a filter clause.
type Predicate = Box<dyn Fn(&Map<String, Value>) -> bool>;

/// A generated filter clause: its query string pairs and its predicate.
struct Clause {
    pairs: Vec<(String, String)>,
    test: Predicate,
}

fn decimal(value: &Value) -> f64 {
    value.as_str().unwrap().parse().unwrap()
}

/// Compare two present values of a sortable field.
fn compare(field: &str, a: &Value, b: &Value) -> Ordering {
    match field {
        "reading_minutes" => a.as_i64().cmp(&b.as_i64()),
        "score" => decimal(a).total_cmp(&decimal(b)),
        _ => a
            .as_str()
            .unwrap()
            .as_bytes()
            .cmp(b.as_str().unwrap().as_bytes()),
    }
}

fn range_value(rng: &mut Rng, field: &str) -> (String, Value) {
    match field {
        "reading_minutes" => {
            let n = rng.below(22);
            (n.to_string(), json!(n))
        }
        "score" => {
            let n = rng.below(20) as i64 - 10;
            (n.to_string(), json!(format!("{n}.00")))
        }
        "published_on" => {
            let text = format!("2026-0{}-1{}", rng.below(9) + 1, rng.below(10));
            (text.clone(), json!(text))
        }
        _ => {
            let text = word(rng);
            (text.clone(), json!(text))
        }
    }
}

fn clause(rng: &mut Rng, authors: &[String], ids: &[String]) -> Clause {
    let pair = |field: &str, op: &str, value: String| (format!("filter[{field}][{op}]"), value);
    match rng.below(8) {
        0 => {
            let category = rng.pick(&CATEGORIES).to_string();
            let wanted = json!(category);
            Clause {
                pairs: vec![pair("category", "eq", category)],
                test: Box::new(move |f| f.get("category") == Some(&wanted)),
            }
        }
        1 => {
            let a = rng.pick(&CATEGORIES).to_string();
            let b = rng.pick(&CATEGORIES).to_string();
            let wanted = [json!(a), json!(b)];
            Clause {
                pairs: vec![pair("category", "in", a), pair("category", "in", b)],
                test: Box::new(move |f| f.get("category").is_some_and(|v| wanted.contains(v))),
            }
        }
        2 => {
            let featured = rng.chance(50);
            Clause {
                pairs: vec![pair("featured", "ne", featured.to_string())],
                test: Box::new(move |f| f.get("featured") != Some(&json!(featured))),
            }
        }
        3 => {
            let field = *rng.pick(&["category", "featured", "tags", "author", "reading_minutes"]);
            let present = rng.chance(50);
            Clause {
                pairs: vec![pair(field, "exists", present.to_string())],
                test: Box::new(move |f| f.contains_key(field) == present),
            }
        }
        4 => {
            let field = *rng.pick(&["reading_minutes", "score", "published_on", "title"]);
            let op = *rng.pick(&["lt", "lte", "gt", "gte"]);
            let (text, value) = range_value(rng, field);
            Clause {
                pairs: vec![pair(field, op, text)],
                test: Box::new(move |f| {
                    f.get(field).is_some_and(|v| {
                        let order = compare(field, v, &value);
                        match op {
                            "lt" => order == Ordering::Less,
                            "lte" => order != Ordering::Greater,
                            "gt" => order == Ordering::Greater,
                            _ => order != Ordering::Less,
                        }
                    })
                }),
            }
        }
        5 => {
            let tags: Vec<String> = (0..=rng.below(2))
                .map(|_| rng.pick(&TAGS).to_string())
                .collect();
            let pairs = tags
                .iter()
                .map(|t| pair("tags", "has", t.clone()))
                .collect();
            Clause {
                pairs,
                test: Box::new(move |f| {
                    f.get("tags")
                        .and_then(Value::as_array)
                        .is_some_and(|have| tags.iter().all(|t| have.contains(&json!(t))))
                }),
            }
        }
        6 => {
            let author = rng.pick(authors).clone();
            Clause {
                pairs: vec![pair("author", "eq", author.clone())],
                test: Box::new(move |f| {
                    f.get("author").is_some_and(|a| a["id"] == author.as_str())
                }),
            }
        }
        _ => {
            let picked: Vec<String> = (0..=rng.below(5)).map(|_| rng.pick(ids).clone()).collect();
            let pairs = picked
                .iter()
                .map(|id| pair("sys.id", "in", id.clone()))
                .collect();
            Clause {
                pairs,
                test: Box::new(move |f| {
                    f.get("__id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| picked.iter().any(|p| p == id))
                }),
            }
        }
    }
}

fn encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._".contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn random_filters_and_sorts_match_an_in_memory_evaluation() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(
        &test,
        "limits = { max_filters = 4, max_sort_keys = 3 }",
        MODEL,
    )
    .await;
    let mut authors = Vec::new();
    for name in ["ada", "bob", "cy"] {
        authors.push(id_of(
            &fx.create("author", json!({"fields": {"name": name}})).await,
        ));
    }
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let generated: Vec<Map<String, Value>> =
        (0..150).map(|i| article(&mut rng, i, &authors)).collect();
    let ids = create_all(
        &fx.client,
        generated.iter().cloned().map(Value::Object).collect(),
    )
    .await;
    let rows: Vec<Map<String, Value>> = generated
        .into_iter()
        .zip(&ids)
        .map(|(mut fields, id)| {
            fields.insert("__id".into(), json!(id));
            fields
        })
        .collect();
    let sortable = ["title", "reading_minutes", "score", "published_on"];
    for round in 0..150 {
        let clauses: Vec<Clause> = (0..rng.below(4))
            .map(|_| clause(&mut rng, &authors, &ids))
            .collect();
        let mut keys: Vec<(&str, bool)> = Vec::new();
        for _ in 0..=rng.below(3) {
            let field = *rng.pick(&sortable);
            if !keys.iter().any(|(f, _)| *f == field) {
                keys.push((field, rng.chance(50)));
            }
        }
        // One clause per (field, op): repeated keys are one `in` or `has`
        // clause, and refused for the other operators.
        let mut seen = HashSet::new();
        let clauses: Vec<Clause> = clauses
            .into_iter()
            .filter(|c| seen.insert(c.pairs[0].0.clone()))
            .collect();
        let mut query: Vec<String> = clauses
            .iter()
            .flat_map(|c| c.pairs.iter())
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect();
        let sort: Vec<String> = keys
            .iter()
            .map(|(f, desc)| format!("{}{f}", if *desc { "-" } else { "" }))
            .collect();
        query.push(format!("sort={}", sort.join(",")));
        let query = query.join("&");
        let mut expected: Vec<&Map<String, Value>> = rows
            .iter()
            .filter(|row| clauses.iter().all(|c| (c.test)(row)))
            .collect();
        expected.sort_by(|a, b| {
            for (field, desc) in &keys {
                let order = match (a.get(*field), b.get(*field)) {
                    (Some(x), Some(y)) => {
                        let o = compare(field, x, y);
                        if *desc { o.reverse() } else { o }
                    }
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                };
                if order != Ordering::Equal {
                    return order;
                }
            }
            // `entry_id` follows the last key's direction.
            let tie = a["__id"].as_str().cmp(&b["__id"].as_str());
            if keys.last().is_some_and(|(_, desc)| *desc) {
                tie.reverse()
            } else {
                tie
            }
        });
        let expected: Vec<&str> = expected
            .iter()
            .map(|r| r["__id"].as_str().unwrap())
            .collect();
        let limit = rng.below(30) + 1;
        let walked = walk(&fx.client, &query, limit).await;
        let walked: Vec<&str> = walked
            .iter()
            .map(|i| i["sys"]["id"].as_str().unwrap())
            .collect();
        assert_eq!(walked, expected, "round {round}: {query} (limit {limit})");
    }
}

#[tokio::test]
async fn list_queries_report_bad_filters_sorts_and_cursors() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let mut ids = Vec::new();
    for (title, minutes) in [("one", 5), ("two", 7), ("three", 9)] {
        ids.push(id_of(
            &fx.create(
                "article",
                json!({"fields": {"title": title, "reading_minutes": minutes, "score": "1"}}),
            )
            .await,
        ));
    }
    let cases = [
        (
            "filter[nope][eq]=1",
            "invalid_filter",
            "/filter",
            "unknown_field",
        ),
        (
            "filter[legacy_code][eq]=1",
            "invalid_filter",
            "/filter/legacy_code/eq",
            "not_filterable",
        ),
        (
            "filter[category][lt]=news",
            "invalid_filter",
            "/filter/category/lt",
            "not_sortable",
        ),
        (
            "filter[reading_minutes][eq]=x",
            "invalid_filter",
            "/filter/reading_minutes/eq",
            "invalid_value",
        ),
        (
            "filter[category][eq]=unknown",
            "invalid_filter",
            "/filter/category/eq",
            "invalid_value",
        ),
        (
            "filter[title][like]=x",
            "invalid_filter",
            "/filter",
            "unknown_operator",
        ),
        (
            "filter[title][eq]=a&filter[title][eq]=b",
            "invalid_filter",
            "/filter",
            "duplicate_key",
        ),
        ("sort=category", "invalid_sort", "/sort", "not_sortable"),
        (
            "sort=title,score,reading_minutes",
            "invalid_sort",
            "/sort",
            "too_many_keys",
        ),
        (
            "sort=title,-title",
            "invalid_sort",
            "/sort",
            "duplicate_key",
        ),
        (
            "cursor=garbage",
            "invalid_cursor",
            "/cursor",
            "invalid_value",
        ),
        ("other=1", "invalid_query", "", "unknown_parameter"),
        ("limit=0", "invalid_query", "/limit", "invalid_value"),
        ("limit=101", "invalid_query", "/limit", "invalid_value"),
        ("status=gone", "invalid_query", "/status", "invalid_value"),
        (
            "fields=title,nope",
            "invalid_query",
            "/fields",
            "unknown_field",
        ),
    ];
    for (query, code, path, reason) in cases {
        let reply = fx.get(&format!("{LIST}?{query}")).send().await;
        assert_eq!(
            (reply.status, reply.code()),
            (StatusCode::BAD_REQUEST, code),
            "{query}: {:?}",
            reply.body
        );
        assert_eq!(
            reply.details(),
            vec![(path.to_owned(), reason.to_owned())],
            "{query}"
        );
    }
    let unknown = fx.get(&format!("{LIST}?locale=de")).send().await;
    assert_eq!(unknown.code(), "locale_unknown");
    // A cursor belongs to its query.
    let first = fx.get(&format!("{LIST}?limit=1&sort=title")).send().await;
    let cursor = first.body["page"]["next_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let other = fx
        .get(&format!("{LIST}?limit=1&sort=-title&cursor={cursor}"))
        .send()
        .await;
    assert_eq!(other.code(), "invalid_cursor");
    let next = fx
        .get(&format!("{LIST}?limit=1&sort=title&cursor={cursor}"))
        .send()
        .await;
    assert_eq!(next.body["items"][0]["fields"]["title"], "three");
    // Decimal filters normalize to the field's scale; sys.id and status.
    let scored = fx
        .get(&format!("{LIST}?filter[score][eq]=1.0"))
        .send()
        .await;
    assert_eq!(scored.body["items"].as_array().unwrap().len(), 3);
    let picked = fx
        .get(&format!(
            "{LIST}?filter[sys.id][in]={}&filter[sys.id][in]={}&fields=title&sort=title",
            ids[0], ids[1]
        ))
        .send()
        .await;
    assert_eq!(
        picked.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["fields"].clone())
            .collect::<Vec<_>>(),
        vec![json!({"title": "one"}), json!({"title": "two"})]
    );
    execute(
        &fx.db,
        &format!(
            "UPDATE merezha_content.variants SET published_revision = 1, published_at = now(), \
             first_published_at = now() WHERE entry_id = '{}'",
            ids[2]
        ),
    )
    .await;
    for (status, count) in [("published", 1), ("draft", 2), ("changed", 0), ("any", 3)] {
        let reply = fx.get(&format!("{LIST}?status={status}")).send().await;
        assert_eq!(
            reply.body["items"].as_array().unwrap().len(),
            count,
            "{status}"
        );
    }
    let recent = fx
        .get(&format!(
            "{LIST}?filter[sys.updated_at][gte]=2000-01-01T00:00:00Z"
        ))
        .send()
        .await;
    assert_eq!(recent.body["items"].as_array().unwrap().len(), 3);
    assert_eq!(recent.body["page"]["limit"], 25);
}

#[tokio::test]
async fn the_default_order_follows_the_last_save_not_creation() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    let a = fx
        .create("article", json!({"fields": {"title": "A"}}))
        .await
        .body["sys"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let b = fx
        .create("article", json!({"fields": {"title": "B"}}))
        .await
        .body["sys"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let saved = fx
        .put(&http::variant("article", &a, "en"))
        .if_match(1)
        .json(&json!({"fields": {"title": "A2"}}))
        .send()
        .await;
    assert_eq!(saved.status, StatusCode::OK);
    let list = fx.get("/v1/content/manage/entries/article").send().await;
    let ids: Vec<&str> = list.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sys"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![a.as_str(), b.as_str()]);
}

/// Replace the first sort value of `cursor` with `value`.
fn forge(cursor: &str, value: &str) -> String {
    use base64::Engine as _;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut body: Value = serde_json::from_slice(&engine.decode(cursor).unwrap()).unwrap();
    body["k"][0] = json!(value);
    engine.encode(body.to_string())
}

#[tokio::test]
async fn forged_cursor_values_are_invalid_cursor_and_never_database_errors() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    for (title, weight) in [("A", 1.5), ("B", 2.5)] {
        fx.create(
            "article",
            json!({"fields": {"title": title, "weight": weight}}),
        )
        .await;
    }
    for (sort, forged) in [
        (
            "",
            vec![
                "+002026-10-24T09:00:00Z",
                "-000001-01-01T00:00:00Z",
                "2026-10-24T09Z",
                "2026-10-24T09:00Z",
                "2026-10-24T24:00:00Z",
                "2026-10-24T09:00:00.123456789Z",
                "2026-10-24T09:00:00,5Z",
                "20261024T090000Z",
                "9999-12-31T23:59:59Z",
                // The exact form, but a year PostgreSQL refuses (SQLSTATE
                // class 22): mapped, not a database error.
                "0000-01-01T00:00:00.000000Z",
            ],
        ),
        (
            "&sort=weight",
            vec!["1e-400", "1e400", "NaN", "Infinity", "+1", "1E5"],
        ),
    ] {
        let first = fx.get(&format!("{LIST}?limit=1{sort}")).send().await;
        let cursor = first.body["page"]["next_cursor"]
            .as_str()
            .unwrap()
            .to_owned();
        let next = fx
            .get(&format!("{LIST}?limit=1{sort}&cursor={cursor}"))
            .send()
            .await;
        assert_eq!(next.status, StatusCode::OK, "{:?}", next.body);
        for value in forged {
            let reply = fx
                .get(&format!(
                    "{LIST}?limit=1{sort}&cursor={}",
                    forge(&cursor, value)
                ))
                .send()
                .await;
            assert_eq!(
                (reply.status, reply.code()),
                (StatusCode::BAD_REQUEST, "invalid_cursor"),
                "{value}: {:?}",
                reply.body
            );
            assert_eq!(
                reply.details(),
                vec![("/cursor".to_owned(), "invalid_value".to_owned())]
            );
        }
    }
}

#[tokio::test]
async fn sorts_in_either_direction_scan_their_index() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", MODEL).await;
    execute(
        &fx.db,
        "WITH e AS (
           INSERT INTO merezha_content.entries (id, scope, content_type, created_at, created_by)
           SELECT gen_random_uuid(), 'app', 'article', now(), 'sys_test'
             FROM generate_series(1, 3000) RETURNING id
         ), v AS (
           INSERT INTO merezha_content.variants
               (entry_id, locale, scope, content_type, draft_revision, draft_data,
                draft_updated_at, draft_updated_by)
           SELECT id, 'en', 'app', 'article', 1,
                  jsonb_build_object('title', md5(id::text),
                                     'reading_minutes', (random() * 599)::int + 1),
                  now() - random() * interval '30 days', 'sys_test'
             FROM e RETURNING entry_id, draft_data
         )
         INSERT INTO merezha_content.revisions
             (entry_id, locale, revision, model_version, kind, data, created_at, created_by)
         SELECT entry_id, 'en', 1, 1, 'create', draft_data, now(), 'sys_test' FROM v;
         ANALYZE merezha_content.entries;
         ANALYZE merezha_content.variants;
         ANALYZE merezha_content.revisions;",
    )
    .await;
    let state = fx.handle.state().unwrap();
    let index = |purpose: merezha_content::IndexPurpose| {
        merezha_content::derived_indexes(state.model())
            .into_iter()
            .find(|i| {
                i.field == "reading_minutes"
                    && i.table == merezha_content::IndexTable::Variants
                    && i.purpose == purpose
            })
            .unwrap()
            .name
    };
    for (query, expected) in [
        (
            "sort=reading_minutes",
            index(merezha_content::IndexPurpose::Sort),
        ),
        (
            "sort=-reading_minutes",
            index(merezha_content::IndexPurpose::SortDescending),
        ),
        ("", "variants_by_update".to_owned()),
        ("sort=sys.updated_at", "variants_by_update".to_owned()),
    ] {
        let plan = state.explain_list("article", query).await.unwrap();
        assert!(
            plan.contains(&format!("Index Scan using {expected}"))
                || plan.contains(&format!("Index Scan Backward using {expected}")),
            "{query}: {plan}"
        );
        assert!(!plan.contains("Sort Key"), "{query} sorts: {plan}");
    }
}
