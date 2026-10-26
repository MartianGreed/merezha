//! Population (section 5, field selection and population): `populate`
//! paths, the entry references a stored value holds, and the batched
//! expansion of those references, one query per level in the stage and
//! locale chain of the request.
//!
//! A path segment names a top-level field of the types the previous level
//! reached (the root type first). Populating a field expands every entry
//! reference inside its value: a `reference`, the references inside a
//! `component`, a `list` of components or `blocks`, and a rich text
//! field's `entry_link` annotations and embedded components. Only targets
//! the caller may read (each under its own type's rule), in the root's
//! scope or the application scope, and visible in the stage (published, or
//! a live draft for preview) are expanded. Every other reference the
//! response checks becomes `{"id", "type", "missing": true}`.
//!
//! Delivery also expands every asset reference of every entry it shows
//! (issue #46): `{"id", "alt"?}` gains `asset`, the metadata
//! `{id, filename, content_type, size, width, height, url}` read for the
//! whole response in one batched statement ([`IDS_PER_STATEMENT`] pairs
//! each), or `null` when the asset is not one the asset route would serve
//! to this reader: trashed (a forced delete), in a scope the entry may not
//! reference, or not recorded in the entry's refs of the stage read.
use crate::{
    model::{ContentModel, ContentType, Field, FieldKind},
    query::detail,
    statements,
    store::{Rejection, parse_object, sql},
};
use merezha_domain::Failure;
use merezha_platform::{Database, FieldError, Tx};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use uuid::Uuid;

/// Ids per population or visibility statement; more are read in chunks.
pub const IDS_PER_STATEMENT: usize = 1000;
/// The code past `limits.max_populated_entries`.
pub const POPULATE_LIMIT: &str = "populate_limit";
/// The code of a response past `limits.max_response_bytes`.
pub const RESPONSE_TOO_LARGE: &str = "response_too_large";
/// Deepest component nesting the reference walker follows; the model
/// bounds nesting at 4.
const MAX_WALK_DEPTH: usize = 8;

/// The `populate` paths as a tree: each segment a field name, its subtree
/// what to populate in the entries it reaches.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Paths(pub BTreeMap<String, Paths>);
impl Paths {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Parse `populate=a,b.c`: at most `max_depth` segments per path, each a
/// field of the types the path reached that can hold entry references,
/// the first one among `selection` when `fields` limits the response.
pub(crate) fn parse_paths(
    model: &ContentModel,
    root: &ContentType,
    text: &str,
    max_depth: u32,
    selection: Option<&[String]>,
) -> Result<Paths, Vec<FieldError>> {
    let mut paths = Paths::default();
    let mut problems = Vec::new();
    for path in text.split(',') {
        let segments: Vec<&str> = path.split('.').collect();
        if path.is_empty() || segments.iter().any(|s| s.is_empty()) {
            problems.push(detail("/populate", "invalid_value"));
            continue;
        }
        if segments.len() > max_depth as usize {
            problems.push(detail("/populate", "too_deep"));
            continue;
        }
        if selection.is_some_and(|s| !s.iter().any(|f| f == segments[0])) {
            problems.push(detail("/populate", "not_selected"));
            continue;
        }
        let mut types: Vec<&ContentType> = vec![root];
        let mut node = &mut paths;
        for segment in &segments {
            let mut next = BTreeSet::new();
            let mut found = false;
            for content_type in &types {
                if let Some(targets) = content_type
                    .field(segment)
                    .and_then(|f| ref_targets(model, &f.kind, 0))
                {
                    found = true;
                    next.extend(targets);
                }
            }
            if !found {
                problems.push(detail("/populate", "unknown_field"));
                break;
            }
            types = next.iter().filter_map(|t| model.content_type(t)).collect();
            node = node.0.entry((*segment).to_owned()).or_default();
        }
    }
    if problems.is_empty() {
        Ok(paths)
    } else {
        problems.dedup();
        Err(problems)
    }
}

/// The types the entry references inside a value of `kind` may target,
/// `None` when it holds none.
pub(crate) fn ref_targets(
    model: &ContentModel,
    kind: &FieldKind,
    depth: usize,
) -> Option<BTreeSet<String>> {
    if depth > MAX_WALK_DEPTH {
        return None;
    }
    let fields = |fields: &[Field]| {
        let mut out: Option<BTreeSet<String>> = None;
        for field in fields {
            if let Some(found) = ref_targets(model, &field.kind, depth + 1) {
                out.get_or_insert_default().extend(found);
            }
        }
        out
    };
    let components = |names: &mut dyn Iterator<Item = &String>| {
        let mut out: Option<BTreeSet<String>> = None;
        for name in names {
            if let Some(found) = model.component(name).and_then(|c| fields(&c.fields)) {
                out.get_or_insert_default().extend(found);
            }
        }
        out
    };
    match kind {
        FieldKind::Reference(rules) => Some(rules.to.iter().cloned().collect()),
        FieldKind::Component(name) => components(&mut std::iter::once(name)),
        FieldKind::List(list) => ref_targets(model, &list.of.kind, depth + 1),
        FieldKind::Blocks(blocks) => components(&mut blocks.components.iter()),
        FieldKind::RichText(rules) => {
            let mut out = components(&mut rules.embeds.iter().filter(|e| *e != "asset"));
            if rules.annotations.iter().any(|a| a == "entry_link") {
                let targets: BTreeSet<String> = match &rules.entry_link_to {
                    Some(to) => to.iter().cloned().collect(),
                    None => model.types().keys().cloned().collect(),
                };
                out.get_or_insert_default().extend(targets);
            }
            out
        }
        _ => None,
    }
}

/// The types the fields of `content_type` may reference directly.
pub(crate) fn referenced_types(
    model: &ContentModel,
    content_type: &ContentType,
) -> BTreeSet<String> {
    content_type
        .fields
        .iter()
        .filter_map(|f| ref_targets(model, &f.kind, 0))
        .flatten()
        .collect()
}

/// Every type a response rooted at `content_type` may show: the type,
/// and the types reachable from it through references, transitively.
pub(crate) fn reachable_types(
    model: &ContentModel,
    content_type: &ContentType,
) -> BTreeSet<String> {
    let mut out = BTreeSet::from([content_type.name.clone()]);
    let mut pending = vec![content_type.name.clone()];
    while let Some(name) = pending.pop() {
        let Some(t) = model.content_type(&name) else {
            continue;
        };
        for target in referenced_types(model, t) {
            if out.insert(target.clone()) {
                pending.push(target);
            }
        }
    }
    out
}

/// Whether a value of `kind` may hold asset references: an `asset`
/// field, an `asset` rich text embed, or either inside a component.
pub(crate) fn holds_assets(model: &ContentModel, kind: &FieldKind, depth: usize) -> bool {
    if depth > MAX_WALK_DEPTH {
        return false;
    }
    let component = |name: &String| {
        model.component(name).is_some_and(|c| {
            c.fields
                .iter()
                .any(|f| holds_assets(model, &f.kind, depth + 1))
        })
    };
    match kind {
        FieldKind::Asset(_) => true,
        FieldKind::Component(name) => component(name),
        FieldKind::List(list) => holds_assets(model, &list.of.kind, depth + 1),
        FieldKind::Blocks(blocks) => blocks.components.iter().any(component),
        FieldKind::RichText(rules) => rules.embeds.iter().any(|e| e == "asset" || component(e)),
        _ => false,
    }
}

/// Which reference objects a walk visits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refs {
    /// Entry references: `reference` values and `entry_link` annotations.
    Entries,
    /// Asset references: `asset` values and `asset` rich text embeds.
    Assets,
}

/// Call `visit` on every entry reference object inside `value`, a stored
/// value of `kind`.
pub(crate) fn visit_refs(
    model: &ContentModel,
    kind: &FieldKind,
    value: &mut Value,
    depth: usize,
    visit: &mut dyn FnMut(&mut Map<String, Value>),
) {
    walk(model, Refs::Entries, kind, value, depth, visit);
}

/// Call `visit` on every asset reference object inside `value`, a stored
/// value of `kind`.
pub(crate) fn visit_assets(
    model: &ContentModel,
    kind: &FieldKind,
    value: &mut Value,
    depth: usize,
    visit: &mut dyn FnMut(&mut Map<String, Value>),
) {
    walk(model, Refs::Assets, kind, value, depth, visit);
}

fn walk(
    model: &ContentModel,
    refs: Refs,
    kind: &FieldKind,
    value: &mut Value,
    depth: usize,
    visit: &mut dyn FnMut(&mut Map<String, Value>),
) {
    if depth > MAX_WALK_DEPTH {
        return;
    }
    let leaf = match kind {
        FieldKind::Reference(_) => refs == Refs::Entries,
        FieldKind::Asset(_) => refs == Refs::Assets,
        _ => false,
    };
    if leaf {
        match value {
            Value::Object(object) => visit(object),
            Value::Array(items) => {
                for item in items {
                    if let Value::Object(object) = item {
                        visit(object);
                    }
                }
            }
            _ => {}
        }
        return;
    }
    match kind {
        FieldKind::Component(name) => {
            if let (Some(component), Value::Object(object)) = (model.component(name), value) {
                visit_fields(model, refs, &component.fields, object, depth + 1, visit);
            }
        }
        FieldKind::List(list) => {
            if let Value::Array(items) = value {
                for item in items {
                    walk(model, refs, &list.of.kind, item, depth + 1, visit);
                }
            }
        }
        FieldKind::Blocks(_) => {
            if let Value::Array(items) = value {
                for item in items {
                    embedded(model, refs, item, depth, visit);
                }
            }
        }
        FieldKind::RichText(_) => {
            let Value::Array(blocks) = value else {
                return;
            };
            for block in blocks {
                let tag = block.get("_type").and_then(Value::as_str);
                if tag == Some("asset") {
                    if let (Refs::Assets, Value::Object(object)) = (refs, block) {
                        visit(object);
                    }
                    continue;
                }
                if tag != Some("block") {
                    embedded(model, refs, block, depth, visit);
                    continue;
                }
                if refs == Refs::Assets {
                    continue;
                }
                let Some(Value::Array(defs)) = block.get_mut("markDefs") else {
                    continue;
                };
                for def in defs {
                    if let Value::Object(object) = def
                        && object.get("_type").and_then(Value::as_str) == Some("entry_link")
                    {
                        visit(object);
                    }
                }
            }
        }
        _ => {}
    }
}

/// A `blocks` item or an embedded rich text object: a component named by
/// its `_type`.
fn embedded(
    model: &ContentModel,
    refs: Refs,
    item: &mut Value,
    depth: usize,
    visit: &mut dyn FnMut(&mut Map<String, Value>),
) {
    let Value::Object(object) = item else {
        return;
    };
    let Some(component) = object
        .get("_type")
        .and_then(Value::as_str)
        .and_then(|name| model.component(name))
    else {
        return;
    };
    visit_fields(model, refs, &component.fields, object, depth + 1, visit);
}

fn visit_fields(
    model: &ContentModel,
    refs: Refs,
    fields: &[Field],
    object: &mut Map<String, Value>,
    depth: usize,
    visit: &mut dyn FnMut(&mut Map<String, Value>),
) {
    for field in fields {
        if let Some(value) = object.get_mut(&field.name) {
            walk(model, refs, &field.kind, value, depth, visit);
        }
    }
}

/// The `(id, type)` of a stored entry reference.
fn target(object: &Map<String, Value>) -> Option<(String, String)> {
    let id = object.get("id")?.as_str()?;
    let content_type = object.get("type")?.as_str()?;
    let id = Uuid::try_parse(id).ok()?.hyphenated().to_string();
    Some((id, content_type.to_owned()))
}

/// The id of a stored asset reference, hyphenated.
fn asset_target(object: &Map<String, Value>) -> Option<String> {
    let id = object.get("id")?.as_str()?;
    Some(Uuid::try_parse(id).ok()?.hyphenated().to_string())
}

/// Which stage the response reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Read {
    /// `published` rows, the delivery default.
    Published,
    /// Live drafts through the chain: preview and management reads.
    Draft,
}

/// One entry as delivery serves it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Doc {
    pub id: String,
    pub content_type: String,
    /// The variant actually served.
    pub source_locale: String,
    /// The published revision, or the draft revision for preview.
    pub revision: i64,
    pub data: Map<String, Value>,
    pub published_at: Option<String>,
    pub first_published_at: Option<String>,
    pub scope: String,
}
impl Doc {
    /// A row of the statements' document columns.
    pub(crate) fn from_row(row: &merezha_postgres::tokio_postgres::Row) -> Result<Self, Rejection> {
        Ok(Self {
            id: row.get(0),
            content_type: row.get(1),
            source_locale: row.get(2),
            revision: row.get(3),
            data: parse_object(row.get(4))?,
            published_at: row.get(5),
            first_published_at: row.get(6),
            scope: row.get(7),
        })
    }
}

/// What population needs to know about the request.
#[derive(Clone, Debug)]
pub(crate) struct Population {
    pub model: std::sync::Arc<ContentModel>,
    pub read: Read,
    /// The requested locale.
    pub locale: String,
    /// The chain of a localized type in that locale, itself first and the
    /// default locale last (drafts only).
    pub chain: Vec<String>,
    pub fallback: bool,
    /// The types the caller may read here, each with the one scope its
    /// entries may come from (the root's scope or `app`).
    pub readable: BTreeMap<String, String>,
    /// `limits.max_populated_entries`.
    pub max_entries: usize,
    /// `limits.max_response_bytes`: the rendered populated entries, and
    /// the response they go into, fail `response_too_large` past it.
    pub max_bytes: usize,
    /// Mark every reference whose target is not visible as `missing`
    /// (delivery); otherwise only those on populated paths (management).
    pub mark_all: bool,
    /// Expand asset references (delivery); management reads show them as
    /// stored.
    pub assets: Option<AssetExpansion>,
}

/// How delivery expands asset references (issue #46).
#[derive(Clone, Debug, Default)]
pub(crate) struct AssetExpansion {
    /// `delivery.public_base_url`: asset URLs are absolute under it, paths
    /// without it.
    pub base: Option<String>,
}

/// The asset metadata of one response: by referring entry and the scope
/// its refs were recorded in, each asset's `asset` member.
type AssetMetadata = HashMap<(String, String), HashMap<String, Value>>;

/// A populated document and what to populate inside it.
struct Node {
    doc: Doc,
    paths: Paths,
    /// `(field, target id)` to the populated node.
    links: HashMap<(String, String), usize>,
}

/// The documents of a response after population, rendered on demand.
#[derive(Debug)]
pub(crate) struct Expanded {
    /// The roots, references marked and linked, in request order.
    pub roots: Vec<Doc>,
    /// Rendered populated entries, indexed by the placeholders in the
    /// roots' (and each other's) `entry` members.
    rendered: Vec<Option<String>>,
}

impl Population {
    /// Populate `paths` below `roots` and mark the references the response
    /// may not show, all inside `tx`.
    pub(crate) async fn run(
        &self,
        db: &Database,
        tx: &mut Tx<'_>,
        roots: Vec<Doc>,
        paths: &Paths,
    ) -> Result<Expanded, Rejection> {
        let root_count = roots.len();
        let mut nodes: Vec<Node> = roots
            .into_iter()
            .map(|doc| Node {
                doc,
                paths: paths.clone(),
                links: HashMap::new(),
            })
            .collect();
        // Entries known visible: every populated one.
        let mut visible: HashSet<String> = HashSet::new();
        let mut frontier: Vec<usize> = (0..root_count).filter(|_| !paths.is_empty()).collect();
        while !frontier.is_empty() {
            // (node, field, id, type) of every reference to expand.
            let mut wants: Vec<(usize, String, String, String)> = Vec::new();
            for &index in &frontier {
                let node = &mut nodes[index];
                let Some(content_type) = self.model.content_type(&node.doc.content_type) else {
                    continue;
                };
                let mut seen = HashSet::new();
                for segment in node.paths.0.keys() {
                    let (Some(field), Some(value)) =
                        (content_type.field(segment), node.doc.data.get_mut(segment))
                    else {
                        continue;
                    };
                    visit_refs(&self.model, &field.kind, value, 0, &mut |object| {
                        if let Some((id, kind)) = target(object)
                            && seen.insert((segment.clone(), id.clone()))
                        {
                            wants.push((index, segment.clone(), id, kind));
                        }
                    });
                }
            }
            wants.retain(|w| self.readable.contains_key(&w.3));
            let populated = nodes.len() - root_count;
            if populated + wants.len() > self.max_entries {
                return Err(Failure::permanent(POPULATE_LIMIT).into());
            }
            let ids: Vec<String> = wants
                .iter()
                .map(|w| w.2.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let fetched = self.fetch(db, tx, &ids).await?;
            let mut next = Vec::new();
            for (index, segment, id, kind) in wants {
                let Some(doc) = fetched.get(&id).filter(|d| d.content_type == kind) else {
                    continue;
                };
                visible.insert(id.clone());
                let sub = nodes[index]
                    .paths
                    .0
                    .get(&segment)
                    .cloned()
                    .unwrap_or_default();
                let child = nodes.len();
                let more = !sub.is_empty();
                nodes.push(Node {
                    doc: doc.clone(),
                    paths: sub,
                    links: HashMap::new(),
                });
                nodes[index].links.insert((segment, id), child);
                if more {
                    next.push(child);
                }
            }
            frontier = next;
        }

        // References to check: every one (delivery) or those on populated
        // paths, minus the populated ones.
        let mut unknown: BTreeSet<String> = BTreeSet::new();
        for node in &mut nodes {
            let Some(content_type) = self.model.content_type(&node.doc.content_type) else {
                continue;
            };
            for field in &content_type.fields {
                if !self.mark_all && !node.paths.0.contains_key(&field.name) {
                    continue;
                }
                let Some(value) = node.doc.data.get_mut(&field.name) else {
                    continue;
                };
                visit_refs(&self.model, &field.kind, value, 0, &mut |object| {
                    if let Some((id, kind)) = target(object)
                        && !node.links.contains_key(&(field.name.clone(), id.clone()))
                        && !visible.contains(&id)
                        && self.readable.contains_key(&kind)
                    {
                        unknown.insert(id);
                    }
                });
            }
        }
        let unknown: Vec<String> = unknown.into_iter().collect();
        let shown = self.visible(db, tx, &unknown).await?;
        if let Some(expansion) = &self.assets {
            self.expand_assets(db, tx, &mut nodes, expansion).await?;
        }

        // Mark and link, children before parents: a child always comes
        // after its parent.
        let mut rendered: Vec<Option<String>> = vec![None; nodes.len()];
        let mut rendered_bytes = 0usize;
        for index in (0..nodes.len()).rev() {
            let node = &mut nodes[index];
            let Some(content_type) = self.model.content_type(&node.doc.content_type) else {
                continue;
            };
            for field in &content_type.fields {
                let checked = self.mark_all || node.paths.0.contains_key(&field.name);
                let Some(value) = node.doc.data.get_mut(&field.name) else {
                    continue;
                };
                visit_refs(&self.model, &field.kind, value, 0, &mut |object| {
                    let Some((id, kind)) = target(object) else {
                        return;
                    };
                    if let Some(child) = node.links.get(&(field.name.clone(), id.clone())) {
                        object.insert("entry".into(), Value::String(placeholder(*child)));
                    } else if checked {
                        let seen = self.readable.contains_key(&kind)
                            && (visible.contains(&id) || shown.contains(&(id, kind)));
                        if !seen {
                            object.insert("missing".into(), Value::Bool(true));
                        }
                    }
                });
            }
            if index >= root_count {
                let mut out = String::new();
                write_populated(&node.doc, content_type, &self.locale, &rendered, &mut out);
                // Each entry counts with the entries rendered inside it, an
                // upper bound on what the response holds.
                rendered_bytes = rendered_bytes.saturating_add(out.len());
                if rendered_bytes > self.max_bytes {
                    return Err(Failure::permanent(RESPONSE_TOO_LARGE).into());
                }
                rendered[index] = Some(out);
            }
        }
        Ok(Expanded {
            roots: nodes.drain(..root_count).map(|n| n.doc).collect(),
            rendered,
        })
    }

    /// Give every asset reference of `nodes` its `asset` member: the
    /// metadata of the asset when the asset route would serve it to this
    /// reader (live, in the entry's scope or the application's, and
    /// recorded in the entry's refs of the stage read), `null` otherwise.
    /// Only entries of types the caller reads here, in the scope its type
    /// allows, are looked up; one statement per [`IDS_PER_STATEMENT`]
    /// `(entry, asset)` pairs, whatever the page size or population.
    async fn expand_assets(
        &self,
        db: &Database,
        tx: &mut Tx<'_>,
        nodes: &mut [Node],
        expansion: &AssetExpansion,
    ) -> Result<(), Rejection> {
        let mut pairs: BTreeSet<(String, String)> = BTreeSet::new();
        for node in nodes.iter_mut() {
            let doc = &mut node.doc;
            if self.readable.get(&doc.content_type) != Some(&doc.scope) {
                continue;
            }
            let Some(content_type) = self.model.content_type(&doc.content_type) else {
                continue;
            };
            let entry = doc.id.clone();
            for field in &content_type.fields {
                if let Some(value) = doc.data.get_mut(&field.name) {
                    visit_assets(&self.model, &field.kind, value, 0, &mut |object| {
                        if let Some(asset) = asset_target(object) {
                            pairs.insert((entry.clone(), asset));
                        }
                    });
                }
            }
        }
        let found = if pairs.is_empty() {
            AssetMetadata::new()
        } else {
            self.asset_metadata(db, tx, pairs, expansion).await?
        };
        for node in nodes.iter_mut() {
            let doc = &mut node.doc;
            let Some(content_type) = self.model.content_type(&doc.content_type) else {
                continue;
            };
            let assets = found.get(&(doc.id.clone(), doc.scope.clone()));
            for field in &content_type.fields {
                if let Some(value) = doc.data.get_mut(&field.name) {
                    visit_assets(&self.model, &field.kind, value, 0, &mut |object| {
                        let metadata = asset_target(object)
                            .and_then(|asset| assets.and_then(|a| a.get(&asset)))
                            .cloned()
                            .unwrap_or(Value::Null);
                        object.insert("asset".into(), metadata);
                    });
                }
            }
        }
        Ok(())
    }

    /// Read the metadata of the assets in `pairs` that the refs of their
    /// entries record, in [`IDS_PER_STATEMENT`] pairs per statement.
    async fn asset_metadata(
        &self,
        db: &Database,
        tx: &mut Tx<'_>,
        pairs: BTreeSet<(String, String)>,
        expansion: &AssetExpansion,
    ) -> Result<AssetMetadata, Rejection> {
        let stage = match self.read {
            Read::Published => "published",
            Read::Draft => "draft",
        };
        let (scopes, _, _) = self.scopes_and_types();
        let pairs: Vec<(String, String)> = pairs.into_iter().collect();
        let mut out = AssetMetadata::new();
        for chunk in pairs.chunks(IDS_PER_STATEMENT) {
            let entries: Vec<String> = chunk
                .iter()
                .map(|p| p.0.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let assets: Vec<String> = chunk
                .iter()
                .map(|p| p.1.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let rows = db
                .query(
                    tx,
                    &statements::DELIVERY_ASSETS,
                    &[&entries, &assets, &stage, &scopes],
                )
                .await
                .map_err(|e| sql(&e))?;
            for row in &rows {
                let (entry, scope, id, asset_scope): (String, String, String, String) =
                    (row.get(0), row.get(1), row.get(2), row.get(3));
                // An entry references its own scope's assets and the
                // application's, never another organization's.
                if asset_scope != scope && asset_scope != "app" {
                    continue;
                }
                let filename: String = row.get(4);
                let path = crate::media::delivery_path(&scope, &id, &filename);
                let url = match &expansion.base {
                    Some(base) => format!("{base}{path}"),
                    None => path,
                };
                let metadata = serde_json::json!({
                    "id": id,
                    "filename": filename,
                    "content_type": row.get::<_, String>(5),
                    "size": row.get::<_, i64>(6),
                    "width": row.get::<_, Option<i32>>(7),
                    "height": row.get::<_, Option<i32>>(8),
                    "url": url,
                });
                out.entry((entry, scope)).or_default().insert(id, metadata);
            }
        }
        Ok(out)
    }

    /// The scopes and types population may read.
    fn scopes_and_types(&self) -> (Vec<String>, Vec<String>, Vec<String>) {
        let scopes: BTreeSet<String> = self.readable.values().cloned().collect();
        let types: Vec<String> = self.readable.keys().cloned().collect();
        let localized: Vec<String> = self
            .readable
            .keys()
            .filter(|t| self.model.content_type(t).is_some_and(|t| t.localized))
            .cloned()
            .collect();
        (scopes.into_iter().collect(), types, localized)
    }

    /// The visible documents among `ids`, by id, each in the scope its
    /// type allows.
    async fn fetch(
        &self,
        db: &Database,
        tx: &mut Tx<'_>,
        ids: &[String],
    ) -> Result<HashMap<String, Doc>, Rejection> {
        let mut out = HashMap::new();
        if ids.is_empty() || self.readable.is_empty() {
            return Ok(out);
        }
        let (scopes, types, localized) = self.scopes_and_types();
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let chunk: Vec<String> = chunk.to_vec();
            let rows = match self.read {
                Read::Published => db
                    .query(
                        tx,
                        &statements::POPULATE_PUBLISHED,
                        &[
                            &chunk,
                            &self.locale,
                            &scopes,
                            &types,
                            &self.fallback,
                            &localized,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?,
                Read::Draft => db
                    .query(
                        tx,
                        &statements::POPULATE_DRAFT,
                        &[
                            &chunk,
                            &self.chain,
                            &scopes,
                            &types,
                            &self.fallback,
                            &localized,
                            &self.locale,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?,
            };
            for row in &rows {
                let doc = Doc::from_row(row)?;
                if self.readable.get(&doc.content_type) == Some(&doc.scope) {
                    out.insert(doc.id.clone(), doc);
                }
            }
        }
        Ok(out)
    }

    /// `(id, type)` of the visible entries among `ids`.
    async fn visible(
        &self,
        db: &Database,
        tx: &mut Tx<'_>,
        ids: &[String],
    ) -> Result<HashSet<(String, String)>, Rejection> {
        let mut out = HashSet::new();
        if ids.is_empty() || self.readable.is_empty() {
            return Ok(out);
        }
        let (scopes, types, localized) = self.scopes_and_types();
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let chunk: Vec<String> = chunk.to_vec();
            let rows = match self.read {
                Read::Published => db
                    .query(
                        tx,
                        &statements::VISIBLE_PUBLISHED,
                        &[
                            &chunk,
                            &self.locale,
                            &scopes,
                            &types,
                            &self.fallback,
                            &localized,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?,
                Read::Draft => db
                    .query(
                        tx,
                        &statements::VISIBLE_DRAFT,
                        &[
                            &chunk,
                            &self.chain,
                            &scopes,
                            &types,
                            &self.fallback,
                            &localized,
                            &self.locale,
                        ],
                    )
                    .await
                    .map_err(|e| sql(&e))?,
            };
            for row in &rows {
                let (id, kind, scope): (String, String, String) =
                    (row.get(0), row.get(1), row.get(2));
                if self.readable.get(&kind) == Some(&scope) {
                    out.insert((id, kind));
                }
            }
        }
        Ok(out)
    }
}

impl Expanded {
    /// The `fields` object of root `index` in model order, limited to
    /// `selection`, with populated entries in place.
    pub(crate) fn write_fields(
        &self,
        index: usize,
        content_type: &ContentType,
        selection: Option<&[String]>,
        out: &mut String,
    ) {
        write_fields(
            &self.roots[index].data,
            content_type,
            selection,
            &self.rendered,
            out,
        );
    }
}

/// The marker an `entry` member holds until rendering: U+0000 never
/// appears in stored strings, which refuse it.
fn placeholder(index: usize) -> String {
    format!("\u{0}{index}")
}

/// A populated entry: `sys` with its id, type, locale, source locale and
/// revision, and every field in model order.
fn write_populated(
    doc: &Doc,
    content_type: &ContentType,
    locale: &str,
    rendered: &[Option<String>],
    out: &mut String,
) {
    out.push_str("{\"sys\":{\"id\":");
    out.push_str(&crate::json::string(&doc.id));
    out.push_str(",\"type\":");
    out.push_str(&crate::json::string(&doc.content_type));
    out.push_str(",\"locale\":");
    out.push_str(&crate::json::string(locale));
    out.push_str(",\"source_locale\":");
    out.push_str(&crate::json::string(&doc.source_locale));
    out.push_str(",\"revision\":");
    out.push_str(&doc.revision.to_string());
    out.push_str("},\"fields\":");
    write_fields(&doc.data, content_type, None, rendered, out);
    out.push('}');
}

fn write_fields(
    data: &Map<String, Value>,
    content_type: &ContentType,
    selection: Option<&[String]>,
    rendered: &[Option<String>],
    out: &mut String,
) {
    out.push('{');
    let mut first = true;
    for field in &content_type.fields {
        if selection.is_some_and(|s| !s.contains(&field.name)) {
            continue;
        }
        let Some(value) = data.get(&field.name) else {
            continue;
        };
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&crate::json::string(&field.name));
        out.push(':');
        write_value(value, rendered, out);
    }
    out.push('}');
}

/// Compact JSON of `value`, with each `entry` placeholder replaced by its
/// rendered entry.
fn write_value(value: &Value, rendered: &[Option<String>], out: &mut String) {
    match value {
        Value::Object(map) => {
            out.push('{');
            for (index, (key, value)) in map.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&crate::json::string(key));
                out.push(':');
                let entry = (key == "entry")
                    .then(|| value.as_str())
                    .flatten()
                    .and_then(|s| s.strip_prefix('\u{0}'))
                    .and_then(|n| n.parse::<usize>().ok())
                    .and_then(|n| rendered.get(n).cloned().flatten());
                match entry {
                    Some(entry) => out.push_str(&entry),
                    None => write_value(value, rendered, out),
                }
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_value(item, rendered, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CONTENT_PERMISSIONS, ModelOptions};
    use serde_json::json;

    const MODEL: &str = r#"
version = 1
locales = [{ code = "en", default = true }]
[components.card]
fields = [{ name = "link", type = "reference", to = ["author"] }]
[types.author]
access = "public"
fields = [{ name = "name", type = "string" }, { name = "friend", type = "reference", to = ["author"] }]
[types.article]
access = "public"
fields = [
  { name = "title", type = "string" },
  { name = "author", type = "reference", to = ["author"] },
  { name = "body", type = "rich_text", annotations = ["entry_link"], embeds = ["card"], entry_link_to = ["article"] },
  { name = "cards", type = "blocks", components = ["card"] },
]
"#;

    fn model() -> ContentModel {
        ContentModel::parse(
            MODEL,
            &ModelOptions {
                permissions: &CONTENT_PERMISSIONS,
                media: false,
                organizations: false,
            },
        )
        .unwrap()
    }

    #[test]
    fn paths_follow_reference_targets_and_their_bounds() {
        let model = model();
        let article = model.content_type("article").unwrap();
        let paths = parse_paths(&model, article, "author.friend,body,cards", 2, None).unwrap();
        assert_eq!(
            paths.0.keys().collect::<Vec<_>>(),
            ["author", "body", "cards"]
        );
        assert!(paths.0["author"].0.contains_key("friend"));
        // `body` reaches articles (entry links) and authors (embedded cards).
        assert!(parse_paths(&model, article, "body.friend", 2, None).is_ok());
        assert!(parse_paths(&model, article, "body.author", 2, None).is_ok());
        for (text, code) in [
            ("title", "unknown_field"),
            ("nope", "unknown_field"),
            ("author.friend.friend", "too_deep"),
            ("author..friend", "invalid_value"),
            ("", "invalid_value"),
        ] {
            let problems = parse_paths(&model, article, text, 2, None).unwrap_err();
            assert_eq!(problems[0].code, code, "{text}");
            assert_eq!(problems[0].path, "/populate");
        }
        assert_eq!(
            parse_paths(&model, article, "author", 0, None).unwrap_err()[0].code,
            "too_deep"
        );
        let selection = ["title".to_owned()];
        assert_eq!(
            parse_paths(&model, article, "author", 2, Some(&selection)).unwrap_err()[0].code,
            "not_selected"
        );
    }

    #[test]
    fn the_walker_finds_references_in_rich_text_blocks_and_components() {
        let model = model();
        let article = model.content_type("article").unwrap();
        let mut data = json!({
            "author": {"id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a11", "type": "author"},
            "body": [
                {"_type": "block", "_key": "a", "markDefs": [
                    {"_type": "entry_link", "_key": "l", "id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a12", "type": "article"}
                ], "children": []},
                {"_type": "card", "_key": "c", "link": {"id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a13", "type": "author"}}
            ],
            "cards": [{"_type": "card", "_key": "d", "link": {"id": "0192f7a0-5d1c-7c3e-9b1a-6f0e2c4d8a14", "type": "author"}}]
        });
        let mut found = Vec::new();
        let object = data.as_object_mut().unwrap();
        for field in &article.fields {
            if let Some(value) = object.get_mut(&field.name) {
                visit_refs(&model, &field.kind, value, 0, &mut |r| {
                    found.push(target(r).unwrap().0);
                });
            }
        }
        let ends: Vec<char> = found.iter().map(|id| id.chars().last().unwrap()).collect();
        assert_eq!(ends, ['1', '2', '3', '4']);
    }

    #[test]
    fn placeholders_render_in_place_and_nothing_else_changes() {
        let rendered = vec![None, Some("{\"sys\":{}}".to_owned())];
        let mut out = String::new();
        write_value(
            &json!({"id": "x", "entry": placeholder(1), "other": {"entry": "plain"}}),
            &rendered,
            &mut out,
        );
        assert_eq!(
            out,
            r#"{"entry":{"sys":{}},"id":"x","other":{"entry":"plain"}}"#
        );
    }
}
