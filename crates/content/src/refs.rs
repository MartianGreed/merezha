//! Reference resolution for draft saves (section 2, `reference` and
//! `asset` rules): the target must exist, not be in the trash, have an
//! allowed type and live in the saving entry's scope or the application
//! scope. Lookups filter on those scopes; only a target found nowhere
//! visible is looked up again, as an existence bit, to tell
//! `reference_scope` from `reference_not_found`.
use crate::value::{ReferenceTarget, ValueReference};
use merezha_domain::Failure;
use merezha_platform::{FieldError, Tx};
use merezha_postgres::classify;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// What the database says about a save's references.
#[derive(Debug, Default)]
pub(crate) struct Resolution {
    /// `reference_not_found`, `reference_type`, `reference_scope`,
    /// `asset_not_found` and `asset_type` details.
    pub problems: Vec<FieldError>,
    /// Entry references written without `type`, with the target's type.
    pub types: Vec<(String, Uuid, String)>,
    /// `(path, target kind, target id)` of every resolved reference, the
    /// `refs` rows of the variant.
    pub rows: Vec<(String, &'static str, Uuid)>,
}

struct Found {
    scope: String,
    content_type: String,
    deleted: bool,
}

fn detail(path: &str, code: &str) -> FieldError {
    FieldError::new(path, code).unwrap_or_else(|_| FieldError {
        path: String::new(),
        code: code.to_owned(),
    })
}

/// Resolve `references` of an entry in `scope`.
pub(crate) async fn resolve(
    tx: &mut Tx<'_>,
    scope: &str,
    references: &[ValueReference],
) -> Result<Resolution, Failure> {
    let mut out = Resolution::default();
    if references.is_empty() {
        return Ok(out);
    }
    let mut entry_ids: Vec<String> = Vec::new();
    let mut asset_ids: Vec<String> = Vec::new();
    for reference in references {
        match &reference.target {
            ReferenceTarget::Entry { id, .. } => entry_ids.push(id.hyphenated().to_string()),
            ReferenceTarget::Asset { id, .. } => asset_ids.push(id.hyphenated().to_string()),
        }
    }
    entry_ids.sort();
    entry_ids.dedup();
    asset_ids.sort();
    asset_ids.dedup();
    let mut entries: HashMap<Uuid, Found> = HashMap::new();
    let mut elsewhere: HashSet<Uuid> = HashSet::new();
    if !entry_ids.is_empty() {
        let rows = tx
            .query(
                "SELECT id::text, scope, content_type, deleted_at IS NOT NULL \
                   FROM merezha_content.entries \
                  WHERE id = ANY($1::text[]::uuid[]) AND (scope = $2 OR scope = 'app')",
                &[&entry_ids, &scope],
            )
            .await
            .map_err(|e| classify(&e))?;
        for row in rows {
            if let Ok(id) = Uuid::try_parse(row.get::<_, &str>(0)) {
                entries.insert(
                    id,
                    Found {
                        scope: row.get(1),
                        content_type: row.get(2),
                        deleted: row.get(3),
                    },
                );
            }
        }
        let missing: Vec<String> = entry_ids
            .iter()
            .filter(|id| Uuid::try_parse(id).is_ok_and(|id| !entries.contains_key(&id)))
            .cloned()
            .collect();
        if !missing.is_empty() {
            // Only whether a live entry of another scope has the id; none of
            // its columns leave the statement.
            let rows = tx
                .query(
                    "SELECT id::text FROM merezha_content.entries \
                      WHERE id = ANY($1::text[]::uuid[]) AND deleted_at IS NULL",
                    &[&missing],
                )
                .await
                .map_err(|e| classify(&e))?;
            elsewhere.extend(
                rows.iter()
                    .filter_map(|row| Uuid::try_parse(row.get::<_, &str>(0)).ok()),
            );
        }
    }
    let mut assets: HashMap<Uuid, String> = HashMap::new();
    if !asset_ids.is_empty() {
        let rows = tx
            .query(
                "SELECT id::text, content_type FROM merezha_content.assets \
                  WHERE id = ANY($1::text[]::uuid[]) AND (scope = $2 OR scope = 'app') \
                    AND deleted_at IS NULL",
                &[&asset_ids, &scope],
            )
            .await
            .map_err(|e| classify(&e))?;
        for row in rows {
            if let Ok(id) = Uuid::try_parse(row.get::<_, &str>(0)) {
                assets.insert(id, row.get(1));
            }
        }
    }
    for reference in references {
        let path = reference.path.as_str();
        match &reference.target {
            ReferenceTarget::Entry {
                id,
                content_type,
                allowed,
                ..
            } => match entries.get(id) {
                Some(found) if found.deleted => {
                    out.problems.push(detail(path, "reference_not_found"))
                }
                Some(found)
                    if content_type
                        .as_ref()
                        .is_some_and(|named| *named != found.content_type)
                        || (!allowed.is_empty() && !allowed.contains(&found.content_type)) =>
                {
                    out.problems.push(detail(path, "reference_type"))
                }
                Some(found) => {
                    debug_assert!(found.scope == scope || found.scope == "app");
                    if content_type.is_none() {
                        out.types
                            .push((path.to_owned(), *id, found.content_type.clone()));
                    }
                    out.rows.push((path.to_owned(), "entry", *id));
                }
                None if elsewhere.contains(id) => {
                    out.problems.push(detail(path, "reference_scope"))
                }
                None => out.problems.push(detail(path, "reference_not_found")),
            },
            ReferenceTarget::Asset { id, accept } => match assets.get(id) {
                None => out.problems.push(detail(path, "asset_not_found")),
                Some(media) if !accepts(accept, media) => {
                    out.problems.push(detail(path, "asset_type"))
                }
                Some(_) => out.rows.push((path.to_owned(), "asset", *id)),
            },
        }
    }
    Ok(out)
}

/// Whether `media` matches one of `accept` (exact or `type/*`); an empty
/// list accepts everything.
pub(crate) fn accepts(accept: &[String], media: &str) -> bool {
    accept.is_empty()
        || accept
            .iter()
            .any(|pattern| match pattern.strip_suffix("/*") {
                Some(major) => media
                    .split_once('/')
                    .is_some_and(|(m, _)| m.eq_ignore_ascii_case(major)),
                None => pattern.eq_ignore_ascii_case(media),
            })
}

/// Write `type` into the entry reference at JSON pointer `path` of
/// `fields` (rooted at `/fields`), when the object there is the reference
/// to `id` and lacks it. Keyed arrays are addressed by `_key`, others by
/// index.
pub(crate) fn fill_type(
    fields: &mut Map<String, Value>,
    path: &str,
    id: &Uuid,
    content_type: &str,
) {
    let Some(rest) = path.strip_prefix("/fields/") else {
        return;
    };
    let mut segments = rest
        .split('/')
        .map(|s| s.replace("~1", "/").replace("~0", "~"));
    let Some(first) = segments.next() else {
        return;
    };
    let Some(mut node) = fields.get_mut(&first) else {
        return;
    };
    for segment in segments {
        node = match node {
            Value::Object(map) => match map.get_mut(&segment) {
                Some(next) => next,
                None => return,
            },
            Value::Array(items) => {
                let keyed = items
                    .iter()
                    .position(|i| i.get("_key").and_then(Value::as_str) == Some(segment.as_str()));
                let index = keyed.or_else(|| {
                    segment
                        .bytes()
                        .all(|b| b.is_ascii_digit())
                        .then(|| segment.parse::<usize>().ok())
                        .flatten()
                });
                match index.and_then(|i| items.get_mut(i)) {
                    Some(next) => next,
                    None => return,
                }
            }
            _ => return,
        };
    }
    if let Value::Object(map) = node
        && map.get("id").and_then(Value::as_str) == Some(id.hyphenated().to_string().as_str())
        && !map.contains_key("type")
    {
        map.insert("type".into(), Value::String(content_type.to_owned()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accept_patterns_match_exact_types_and_majors() {
        let accept = vec!["image/*".to_owned(), "application/pdf".to_owned()];
        assert!(accepts(&accept, "image/png"));
        assert!(accepts(&accept, "application/pdf"));
        assert!(!accepts(&accept, "text/plain"));
        assert!(accepts(&[], "text/plain"));
    }
    #[test]
    fn types_are_filled_through_keys_and_indexes() {
        let id = Uuid::nil();
        let nil = id.hyphenated().to_string();
        let mut fields = json!({
            "author": {"id": nil},
            "related": [{"id": nil}, {"id": nil, "type": "x"}],
            "body": [{"_key": "b2", "markDefs": [{"_key": "l2", "_type": "entry_link", "id": nil}]}]
        })
        .as_object()
        .cloned()
        .unwrap();
        fill_type(&mut fields, "/fields/author", &id, "author");
        fill_type(&mut fields, "/fields/related/0", &id, "article");
        fill_type(&mut fields, "/fields/related/1", &id, "article");
        fill_type(&mut fields, "/fields/body/b2/markDefs/l2", &id, "article");
        fill_type(&mut fields, "/fields/missing/0", &id, "article");
        assert_eq!(fields["author"]["type"], "author");
        assert_eq!(fields["related"][0]["type"], "article");
        assert_eq!(fields["related"][1]["type"], "x");
        assert_eq!(fields["body"][0]["markDefs"][0]["type"], "article");
    }
}
