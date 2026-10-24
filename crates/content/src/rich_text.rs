//! Portable Text validation for `rich_text` fields (section 2): a flat
//! array of blocks holding spans, embedded assets and components. Every
//! structural problem is `rich_text_invalid` at the pointer of the
//! offending block, mark definition, span or member.
use crate::{
    model::RichTextRules,
    scalar,
    value::{Ctx, child, valid_key},
};
use serde_json::{Map, Value};
use std::collections::HashSet;

/// Most `markDefs` in one block.
pub const MAX_MARK_DEFS: usize = 100;
/// Most spans in one block.
pub const MAX_SPANS: usize = 1000;
/// Most marks on one span.
pub const MAX_SPAN_MARKS: usize = 16;
/// Longest link `href`, in bytes.
pub const MAX_HREF_BYTES: usize = 2048;

const INVALID: &str = "rich_text_invalid";
const BLOCK_KEYS: [&str; 7] = [
    "_type", "_key", "style", "listItem", "level", "markDefs", "children",
];
const SPAN_KEYS: [&str; 4] = ["_type", "_key", "text", "marks"];

pub(crate) fn validate(
    ctx: &mut Ctx<'_, '_>,
    rules: &RichTextRules,
    value: &Value,
    path: &str,
) -> Option<Value> {
    let Value::Array(blocks) = value else {
        ctx.violation(path, INVALID);
        return None;
    };
    if blocks.len() > rules.max_blocks as usize {
        ctx.violation(path, "max_items");
        return None;
    }
    let mut text_length = 0usize;
    let embeds = |name: &str| rules.embeds.iter().any(|e| e == name);
    let out = ctx.keyed(blocks, path, |ctx, object, at| {
        match object.get("_type").and_then(Value::as_str) {
            Some("block") => block(ctx, rules, object, at, &mut text_length),
            Some("asset") if embeds("asset") => {
                let item = Value::Object(object.clone());
                let stored =
                    ctx.asset_reference(&item, at, &[], &["_type", "_key", "id", "alt"])?;
                tagged(stored, "asset")
            }
            Some(name) if name != "asset" && name != "block" && embeds(name) => {
                let component = ctx.model.component(name)?;
                let mut stored =
                    ctx.object(&component.fields, object, at, None, &["_type", "_key"]);
                stored.insert("_type".into(), Value::String(name.to_owned()));
                Some(stored)
            }
            _ => {
                ctx.violation(at, INVALID);
                None
            }
        }
    });
    if text_length > rules.max_text_length as usize {
        ctx.violation(path, "max_length");
    }
    Some(Value::Array(out))
}

fn tagged(stored: Value, tag: &str) -> Option<Map<String, Value>> {
    let Value::Object(mut map) = stored else {
        return None;
    };
    map.insert("_type".into(), Value::String(tag.to_owned()));
    Some(map)
}

fn block(
    ctx: &mut Ctx<'_, '_>,
    rules: &RichTextRules,
    object: &Map<String, Value>,
    at: &str,
    text_length: &mut usize,
) -> Option<Map<String, Value>> {
    if object.keys().any(|k| !BLOCK_KEYS.contains(&k.as_str())) {
        ctx.violation(at, INVALID);
    }
    let mut stored = Map::new();
    stored.insert("_type".into(), Value::String("block".into()));
    match object.get("style") {
        Some(Value::String(style)) if rules.styles.contains(style) => {
            stored.insert("style".into(), Value::String(style.clone()));
        }
        _ => ctx.violation(&child(at, "style"), INVALID),
    }
    match object.get("listItem") {
        None => {}
        Some(Value::String(kind)) if rules.lists.contains(kind) => {
            stored.insert("listItem".into(), Value::String(kind.clone()));
        }
        Some(_) => ctx.violation(&child(at, "listItem"), INVALID),
    }
    match object.get("level") {
        None => {}
        Some(level)
            if level.as_u64().is_some_and(|l| (1..=6).contains(&l))
                && object.contains_key("listItem") =>
        {
            stored.insert("level".into(), level.clone());
        }
        Some(_) => ctx.violation(&child(at, "level"), INVALID),
    }
    let defs_path = child(at, "markDefs");
    let mut def_keys: HashSet<String> = HashSet::new();
    let mut defs = Vec::new();
    match object.get("markDefs") {
        Some(Value::Array(items)) if items.len() <= MAX_MARK_DEFS => {
            for (index, item) in items.iter().enumerate() {
                if let Some(def) = mark_def(ctx, rules, item, &defs_path, index, &mut def_keys) {
                    defs.push(def);
                }
            }
        }
        _ => ctx.violation(&defs_path, INVALID),
    }
    stored.insert("markDefs".into(), Value::Array(defs));
    let children_path = child(at, "children");
    match object.get("children") {
        Some(Value::Array(spans)) if spans.len() <= MAX_SPANS => {
            let spans = ctx.keyed(spans, &children_path, |ctx, span, span_at| {
                self::span(ctx, rules, span, span_at, &def_keys, text_length)
            });
            stored.insert("children".into(), Value::Array(spans));
        }
        _ => ctx.violation(&children_path, INVALID),
    }
    Some(stored)
}

fn mark_def(
    ctx: &mut Ctx<'_, '_>,
    rules: &RichTextRules,
    item: &Value,
    defs_path: &str,
    index: usize,
    def_keys: &mut HashSet<String>,
) -> Option<Value> {
    let index_path = child(defs_path, &index.to_string());
    let Value::Object(object) = item else {
        ctx.violation(&index_path, INVALID);
        return None;
    };
    // Marks name definitions by key, so a key must be present, unique and
    // distinct from every decorator; it is never generated.
    let key = match object.get("_key") {
        Some(Value::String(key))
            if valid_key(key)
                && !def_keys.contains(key)
                && !crate::model::RICH_TEXT_MARKS.contains(&key.as_str()) =>
        {
            key.clone()
        }
        _ => {
            ctx.violation(&index_path, INVALID);
            return None;
        }
    };
    def_keys.insert(key.clone());
    let at = child(defs_path, &key);
    let annotation = object.get("_type").and_then(Value::as_str);
    let allowed = |name: &str| rules.annotations.iter().any(|a| a == name);
    let mut stored = match annotation {
        Some("link") if allowed("link") => {
            let only_known = object
                .keys()
                .all(|k| ["_type", "_key", "href"].contains(&k.as_str()));
            match object.get("href") {
                Some(Value::String(href)) if only_known && valid_href(href) => {
                    let mut map = Map::new();
                    map.insert("href".into(), Value::String(href.clone()));
                    map
                }
                _ => {
                    ctx.violation(&at, INVALID);
                    return None;
                }
            }
        }
        Some("entry_link") if allowed("entry_link") => {
            let to = rules.entry_link_to.clone().unwrap_or_default();
            let stored =
                ctx.entry_reference(item, &at, &to, false, &["_type", "_key", "id", "type"])?;
            let Value::Object(map) = stored else {
                return None;
            };
            map
        }
        _ => {
            ctx.violation(&at, INVALID);
            return None;
        }
    };
    stored.insert("_type".into(), Value::String(annotation?.to_owned()));
    stored.insert("_key".into(), Value::String(key));
    Some(Value::Object(stored))
}

fn span(
    ctx: &mut Ctx<'_, '_>,
    rules: &RichTextRules,
    object: &Map<String, Value>,
    at: &str,
    def_keys: &HashSet<String>,
    text_length: &mut usize,
) -> Option<Map<String, Value>> {
    let well_formed = object.keys().all(|k| SPAN_KEYS.contains(&k.as_str()))
        && object.get("_type").and_then(Value::as_str) == Some("span");
    if !well_formed {
        ctx.violation(at, INVALID);
        return None;
    }
    let Some(Value::String(text)) = object.get("text") else {
        ctx.violation(&child(at, "text"), INVALID);
        return None;
    };
    if text.contains('\0') {
        ctx.violation(&child(at, "text"), "invalid_character");
        return None;
    }
    *text_length = text_length.saturating_add(scalar::length(text));
    let marks = match object.get("marks") {
        None => Vec::new(),
        Some(Value::Array(marks)) if marks.len() <= MAX_SPAN_MARKS => {
            let mut seen = HashSet::new();
            let valid = marks.iter().all(|m| {
                m.as_str().is_some_and(|m| {
                    (rules.marks.iter().any(|d| d == m) || def_keys.contains(m)) && seen.insert(m)
                })
            });
            if !valid {
                ctx.violation(&child(at, "marks"), INVALID);
                return None;
            }
            marks.clone()
        }
        Some(_) => {
            ctx.violation(&child(at, "marks"), INVALID);
            return None;
        }
    };
    let mut stored = Map::new();
    stored.insert("_type".into(), Value::String("span".into()));
    stored.insert("text".into(), Value::String(text.clone()));
    stored.insert("marks".into(), Value::Array(marks));
    Some(stored)
}

/// An `https`, `http` or `mailto` URL of printable ASCII, at most 2048
/// bytes; web URLs need a host.
pub(crate) fn valid_href(href: &str) -> bool {
    if href.is_empty() || href.len() > MAX_HREF_BYTES || !href.bytes().all(|b| b.is_ascii_graphic())
    {
        return false;
    }
    let after = |scheme: &str| {
        href.get(..scheme.len())
            .filter(|p| p.eq_ignore_ascii_case(scheme))
            .map(|_| &href[scheme.len()..])
    };
    if let Some(rest) = after("https://").or_else(|| after("http://")) {
        return rest
            .split(['/', '?', '#'])
            .next()
            .is_some_and(|host| !host.is_empty());
    }
    after("mailto:").is_some_and(|rest| !rest.is_empty())
}
