//! The jobs admin HTTP API (AP §D.1), nested by the host under `/v1/jobs`:
//! `/v1/jobs/admin/...`. Every route checks its permission at
//! [`ResourceScope::Application`] (R5) through the host's policy before
//! anything else: `jobs.admin.read` for reads (`GET queues`, `jobs`,
//! `jobs/{id}`, `outbox`, `schedules`), `jobs.admin.write` for writes
//! (replay, cancel, purge, requeue, and `schedules/{name}/pause`, `/resume`
//! and `/trigger`), and `jobs.admin.payload` as well to see a payload. Without the
//! identity block the loopback policy allows them and a public listener
//! denies them.
use crate::{
    PgJobs,
    admin::{DeadFilter, JOB_STATES, JobFilter, MAX_BULK_REPLAY},
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Path, Request, State},
    http::{StatusCode, Uri, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use merezha_domain::{Failure, Identity, valid_name};
use merezha_platform::{
    ErrorReply, FieldError,
    config::{valid_queue_name, valid_schedule_name},
    edge::Authenticated,
    ports::{Authorizer, ResourceScope},
};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// Largest admin request body.
const MAX_BODY: usize = 8 * 1024;
const READ: &str = "jobs.admin.read";
const WRITE: &str = "jobs.admin.write";
const PAYLOAD: &str = "jobs.admin.payload";

#[derive(Clone)]
struct Admin {
    jobs: PgJobs,
    policy: Arc<dyn Authorizer>,
}

pub(crate) fn router(jobs: PgJobs, policy: Arc<dyn Authorizer>) -> Router {
    Router::new()
        .route("/admin/queues", get(queues))
        .route("/admin/jobs", get(list))
        .route("/admin/jobs/{id}", get(detail))
        .route("/admin/jobs/{id}/replay", post(replay))
        .route("/admin/jobs/{id}/cancel", post(cancel))
        .route("/admin/dead/replay", post(replay_dead))
        .route("/admin/dead/purge", post(purge))
        .route("/admin/outbox", get(outbox))
        .route("/admin/outbox/{id}/requeue", post(requeue))
        .route("/admin/schedules", get(schedules))
        .route("/admin/schedules/{name}/{action}", post(schedule_action))
        .with_state(Admin { jobs, policy })
}

type Reply = Result<Response, ErrorReply>;

fn failure(failure: Failure) -> ErrorReply {
    ErrorReply::new(failure)
}
fn invalid(details: Vec<FieldError>) -> ErrorReply {
    ErrorReply::new(Failure::permanent("validation_failed")).details(details)
}
/// The response of a handler body.
fn respond(reply: Reply) -> Response {
    reply.unwrap_or_else(IntoResponse::into_response)
}
fn field_error(path: &str, code: &str) -> FieldError {
    FieldError::new(path, code).unwrap_or(FieldError {
        path: String::new(),
        code: "invalid".into(),
    })
}

impl Admin {
    /// Check every permission for the request's principal; returns its
    /// subject for the audit row.
    fn allow(&self, parts: &Parts, permissions: &[&str]) -> Result<Option<String>, ErrorReply> {
        let principal = parts
            .extensions
            .get::<Authenticated>()
            .and_then(|authenticated| authenticated.0.clone());
        for permission in permissions {
            Identity::new(*permission)
                .and_then(|permission| {
                    self.policy
                        .check(
                            principal.as_deref(),
                            &permission,
                            &ResourceScope::Application,
                        )
                        .map_err(Failure::from)
                })
                .map_err(failure)?;
        }
        Ok(principal.map(|p| p.subject.as_str().to_owned()))
    }
}

/// A positive id from the path; anything else is `not_found`.
fn path_id(id: &str) -> Result<i64, ErrorReply> {
    id.parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| failure(Failure::permanent("not_found")))
}

/// `key=value` pairs. Values here are names, numbers and booleans, so
/// percent-encoding is refused rather than decoded.
fn query(uri: &Uri, allowed: &[&str]) -> Result<Vec<(String, String)>, ErrorReply> {
    let mut pairs = Vec::new();
    let mut problems = Vec::new();
    for pair in uri
        .query()
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
    {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !allowed.contains(&key) {
            problems.push(field_error("", "unknown_parameter"));
        } else if pairs.iter().any(|(seen, _): &(String, String)| seen == key) {
            problems.push(field_error(&format!("/{key}"), "duplicate_parameter"));
        } else if value.contains(['%', '+']) {
            problems.push(field_error(&format!("/{key}"), "invalid_value"));
        } else {
            pairs.push((key.to_owned(), value.to_owned()));
        }
    }
    if problems.is_empty() {
        Ok(pairs)
    } else {
        Err(invalid(problems))
    }
}

/// The JSON object body, or an empty one for an empty body.
async fn body(request: Request) -> Result<Map<String, Value>, ErrorReply> {
    let bytes = to_bytes(request.into_body(), MAX_BODY)
        .await
        .map_err(|_| failure(Failure::permanent("body_limit")))?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(Map::new());
    }
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(Value::Object(map)) => Ok(map),
        _ => Err(failure(Failure::permanent("invalid_input"))),
    }
}

fn ok(value: Value) -> Reply {
    Ok(Json(value).into_response())
}

async fn queues(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        admin.allow(&parts, &[READ])?;
        let queues = admin.jobs.queue_summaries().await.map_err(failure)?;
        ok(json!({ "queues": queues }))
    };
    respond(run.await)
}

async fn outbox(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        admin.allow(&parts, &[READ])?;
        let outbox = admin.jobs.outbox_summary().await.map_err(failure)?;
        ok(json!(outbox))
    };
    respond(run.await)
}

async fn list(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        admin.allow(&parts, &[READ])?;
        let mut filter = JobFilter {
            limit: 50,
            ..JobFilter::default()
        };
        let mut problems = Vec::new();
        for (key, value) in query(&parts.uri, &["queue", "state", "kind", "limit", "after"])? {
            let path = format!("/{key}");
            match key.as_str() {
                "queue" if valid_queue_name(&value) => filter.queue = Some(value),
                "state" if JOB_STATES.contains(&value.as_str()) => filter.state = Some(value),
                "kind" if valid_name(&value) => filter.kind = Some(value),
                "limit" => match value.parse::<u32>() {
                    Ok(limit @ 1..=200) => filter.limit = limit,
                    _ => problems.push(field_error(&path, "out_of_range")),
                },
                "after" => match value.parse::<i64>() {
                    Ok(after) if after >= 0 => filter.after = Some(after),
                    _ => problems.push(field_error(&path, "invalid_cursor")),
                },
                _ => problems.push(field_error(&path, "invalid_value")),
            }
        }
        if !problems.is_empty() {
            return Err(invalid(problems));
        }
        let page = admin.jobs.list_jobs(&filter).await.map_err(failure)?;
        ok(json!(page))
    };
    respond(run.await)
}

async fn detail_route(admin: Admin, id: String, request: Request) -> Reply {
    let (parts, _) = request.into_parts();
    admin.allow(&parts, &[READ])?;
    let mut payload = false;
    for (_, value) in query(&parts.uri, &["payload"])? {
        payload = match value.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(invalid(vec![field_error("/payload", "invalid_value")])),
        };
    }
    if payload {
        admin.allow(&parts, &[PAYLOAD])?;
    }
    let id = path_id(&id)?;
    let job = admin.jobs.job(id, payload).await.map_err(failure)?;
    ok(json!(job))
}
async fn detail(State(admin): State<Admin>, Path(id): Path<String>, request: Request) -> Response {
    respond(detail_route(admin, id, request).await)
}

async fn replay(State(admin): State<Admin>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let principal = admin.allow(&parts, &[WRITE])?;
        let id = path_id(&id)?;
        let replayed = admin
            .jobs
            .replay(id, principal.as_deref())
            .await
            .map_err(failure)?;
        ok(json!({ "id": id, "state": replayed.state() }))
    };
    respond(run.await)
}

async fn cancel(State(admin): State<Admin>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let principal = admin.allow(&parts, &[WRITE])?;
        let id = path_id(&id)?;
        admin
            .jobs
            .cancel_job(id, principal.as_deref())
            .await
            .map_err(failure)?;
        ok(json!({ "id": id, "state": "cancelled" }))
    };
    respond(run.await)
}

async fn requeue(State(admin): State<Admin>, Path(id): Path<String>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        let principal = admin.allow(&parts, &[WRITE])?;
        let id = path_id(&id)?;
        admin
            .jobs
            .requeue_outbox(id, principal.as_deref())
            .await
            .map_err(failure)?;
        ok(json!({ "id": id }))
    };
    respond(run.await)
}

/// Reads typed fields of a body, collecting every problem.
struct Fields {
    map: Map<String, Value>,
    problems: Vec<FieldError>,
}
impl Fields {
    fn new(map: Map<String, Value>, allowed: &[&str]) -> Self {
        let problems = map
            .keys()
            .filter(|key| !allowed.contains(&key.as_str()))
            .map(|_| field_error("", "unknown_field"))
            .collect();
        Self { map, problems }
    }
    fn name(&mut self, key: &str, valid: fn(&str) -> bool, required: bool) -> Option<String> {
        match self.map.remove(key) {
            Some(Value::String(value)) if valid(&value) => Some(value),
            None | Some(Value::Null) if !required => None,
            None => {
                self.problems
                    .push(field_error(&format!("/{key}"), "required"));
                None
            }
            Some(_) => {
                self.problems
                    .push(field_error(&format!("/{key}"), "invalid_value"));
                None
            }
        }
    }
    fn timestamp(&mut self, key: &str, required: bool) -> Option<jiff::Timestamp> {
        let text = self.name(key, |v| v.len() <= 64, required)?;
        match text.parse::<jiff::Timestamp>() {
            Ok(at) => Some(at),
            Err(_) => {
                self.problems
                    .push(field_error(&format!("/{key}"), "invalid_timestamp"));
                None
            }
        }
    }
    fn finish(self) -> Result<(), ErrorReply> {
        if self.problems.is_empty() {
            Ok(())
        } else {
            Err(invalid(self.problems))
        }
    }
}

async fn replay_dead(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, body_part) = request.into_parts();
        let principal = admin.allow(&parts, &[WRITE])?;
        let map = body(Request::from_parts(parts, body_part)).await?;
        let mut fields = Fields::new(
            map,
            &[
                "queue",
                "kind",
                "code",
                "dead_after",
                "dead_before",
                "limit",
            ],
        );
        let limit = match fields.map.remove("limit") {
            None => Some(100),
            Some(Value::Number(n)) => n
                .as_u64()
                .filter(|n| (1..=u64::from(MAX_BULK_REPLAY)).contains(n))
                .map(|n| n as u32),
            Some(_) => None,
        };
        if limit.is_none() {
            fields.problems.push(field_error("/limit", "out_of_range"));
        }
        let queue = fields.name("queue", valid_queue_name, true);
        let kind = fields.name("kind", valid_name, false);
        let code = fields.name("code", valid_name, false);
        let dead_after = fields.timestamp("dead_after", false);
        let dead_before = fields.timestamp("dead_before", false);
        fields.finish()?;
        let (Some(queue), Some(limit)) = (queue, limit) else {
            return Err(failure(Failure::permanent("validation_failed")));
        };
        let filter = DeadFilter {
            queue,
            kind,
            code,
            dead_after,
            dead_before,
            limit,
        };
        // Detached from this request: a disconnect or the HTTP drain cannot
        // cut a reissue between claim and hook.
        let (progress, handle) = admin.jobs.spawn_replay_dead(filter, principal);
        match tokio::time::timeout(admin.jobs.reissue_timing().bulk_wait, handle).await {
            Ok(Ok(Ok(replayed))) => ok(json!({
                "replayed": replayed.replayed,
                "reissued": replayed.reissued,
                "refused": replayed.refused,
                "complete": true,
            })),
            Ok(Ok(Err(refused))) => Err(failure(refused)),
            Ok(Err(_)) => Err(failure(Failure::transient("internal"))),
            Err(_) => {
                let so_far = progress.snapshot();
                Ok((
                    StatusCode::ACCEPTED,
                    Json(json!({
                        "replayed": so_far.replayed,
                        "reissued": so_far.reissued,
                        "refused": so_far.refused,
                        "complete": false,
                    })),
                )
                    .into_response())
            }
        }
    };
    respond(run.await)
}

async fn purge(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, body_part) = request.into_parts();
        let principal = admin.allow(&parts, &[WRITE])?;
        let map = body(Request::from_parts(parts, body_part)).await?;
        let mut fields = Fields::new(map, &["queue", "before"]);
        let queue = fields.name("queue", valid_queue_name, true);
        let before = fields.timestamp("before", true);
        fields.finish()?;
        let (Some(queue), Some(before)) = (queue, before) else {
            return Err(failure(Failure::permanent("validation_failed")));
        };
        let purged = admin
            .jobs
            .purge_dead(&queue, before, principal.as_deref())
            .await
            .map_err(failure)?;
        ok(json!({ "purged": purged }))
    };
    respond(run.await)
}

async fn schedules(State(admin): State<Admin>, request: Request) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        admin.allow(&parts, &[READ])?;
        let mut limit = 50;
        let mut after = None;
        let mut problems = Vec::new();
        for (key, value) in query(&parts.uri, &["limit", "after"])? {
            let path = format!("/{key}");
            match key.as_str() {
                "limit" => match value.parse::<u32>() {
                    Ok(n @ 1..=crate::MAX_SCHEDULES_LISTED) => limit = n,
                    _ => problems.push(field_error(&path, "out_of_range")),
                },
                _ if valid_schedule_name(&value) => after = Some(value),
                _ => problems.push(field_error(&path, "invalid_cursor")),
            }
        }
        if !problems.is_empty() {
            return Err(invalid(problems));
        }
        let page = admin
            .jobs
            .list_schedules(after.as_deref(), limit)
            .await
            .map_err(failure)?;
        ok(json!(page))
    };
    respond(run.await)
}

/// `POST schedules/{name}/pause`, `/resume` and `/trigger`.
async fn schedule_action(
    State(admin): State<Admin>,
    Path((name, action)): Path<(String, String)>,
    request: Request,
) -> Response {
    let run = async {
        let (parts, _) = request.into_parts();
        if !matches!(action.as_str(), "pause" | "resume" | "trigger") {
            return Err(failure(Failure::permanent("not_found")));
        }
        let principal = admin.allow(&parts, &[WRITE])?;
        if !valid_schedule_name(&name) {
            return Err(failure(Failure::permanent("not_found")));
        }
        let principal = principal.as_deref();
        match action.as_str() {
            "pause" => ok(json!(
                admin
                    .jobs
                    .pause_schedule(&name, principal)
                    .await
                    .map_err(failure)?
            )),
            "resume" => ok(json!(
                admin
                    .jobs
                    .resume_schedule(&name, principal)
                    .await
                    .map_err(failure)?
            )),
            _ => ok(json!(
                admin
                    .jobs
                    .trigger_schedule(&name, principal)
                    .await
                    .map_err(failure)?
            )),
        }
    };
    respond(run.await)
}
