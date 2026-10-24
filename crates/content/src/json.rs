//! JSON helpers of the management API: top-level bodies with duplicate and
//! unknown keys refused, RFC 7396 merge patches, and `fields` objects
//! written in model order.
use crate::model::ContentType;
use merezha_domain::Failure;
use serde::de::{Deserializer, MapAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;

/// A request body that must be a JSON object whose top-level keys are all
/// in `allowed` and appear once each. Anything else is permanent
/// `invalid_json`.
pub(crate) fn body_object(bytes: &[u8], allowed: &[&str]) -> Result<Map<String, Value>, Failure> {
    let invalid = || Failure::permanent("invalid_json");
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let object = deserializer
        .deserialize_map(TopLevel)
        .map_err(|_| invalid())?;
    deserializer.end().map_err(|_| invalid())?;
    let object = object.ok_or_else(invalid)?;
    if object.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(invalid());
    }
    Ok(object)
}

/// Deserializes an object, `None` when a top-level key repeats.
struct TopLevel;
impl<'de> Visitor<'de> for TopLevel {
    type Value = Option<Map<String, Value>>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let mut out = Map::new();
        let mut duplicate = false;
        while let Some((key, value)) = access.next_entry::<String, Value>()? {
            if out.insert(key, value).is_some() {
                duplicate = true;
            }
        }
        Ok((!duplicate).then_some(out))
    }
}

/// Apply an RFC 7396 merge patch to `target`.
pub(crate) fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let Value::Object(object) = target else {
        return;
    };
    for (key, value) in patch {
        if value.is_null() {
            object.remove(key);
        } else {
            merge_patch(object.entry(key.clone()).or_insert(Value::Null), value);
        }
    }
}

/// Encoded size of `value`, the bound `limits.max_entry_bytes` applies to.
pub(crate) fn encoded_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |b| b.len())
}

/// `fields` as a JSON object in the type's field order, limited to
/// `selection` when given. Members the model does not know are left out.
pub(crate) fn write_fields(
    content_type: &ContentType,
    fields: &Map<String, Value>,
    selection: Option<&[String]>,
    out: &mut String,
) {
    out.push('{');
    let mut first = true;
    for field in &content_type.fields {
        if selection.is_some_and(|s| !s.contains(&field.name)) {
            continue;
        }
        let Some(value) = fields.get(&field.name) else {
            continue;
        };
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&Value::String(field.name.clone()).to_string());
        out.push(':');
        out.push_str(&value.to_string());
    }
    out.push('}');
}

/// A JSON string literal.
pub(crate) fn string(value: &str) -> String {
    Value::String(value.to_owned()).to_string()
}

/// A JSON string literal or `null`.
pub(crate) fn optional(value: Option<&str>) -> String {
    value.map_or_else(|| "null".to_owned(), string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bodies_refuse_duplicates_unknown_keys_and_non_objects() {
        assert!(body_object(br#"{"fields":{}}"#, &["fields"]).is_ok());
        for bad in [
            &br#"{"fields":{},"fields":{}}"#[..],
            br#"{"other":1}"#,
            br#"[1]"#,
            br#"{"fields":{}} x"#,
            b"",
        ] {
            assert_eq!(
                body_object(bad, &["fields"]).unwrap_err().code,
                "invalid_json"
            );
        }
        // Nested duplicates are the field validator's business (last wins).
        assert!(body_object(br#"{"fields":{"a":1,"a":2}}"#, &["fields"]).is_ok());
    }
    #[test]
    fn merge_patches_follow_rfc_7396() {
        let mut target = json!({"a": "b", "c": {"d": "e", "f": "g"}, "l": [1, 2]});
        merge_patch(
            &mut target,
            &json!({"a": "z", "c": {"f": null}, "l": [3], "n": {"x": 1}}),
        );
        assert_eq!(
            target,
            json!({"a": "z", "c": {"d": "e"}, "l": [3], "n": {"x": 1}})
        );
    }
}
