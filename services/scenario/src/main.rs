//! Usage: `task-board-scenario <ADDRESS> <SCRATCH_DIR> <EXAMPLE_DIR>`
//!
//! Drives the task board application on a running host through its HTTP
//! application endpoints and checks every observation. `ADDRESS` is the
//! host's loopback `host:port`. The host must have been started with
//! `SCRATCH_DIR/runtime.toml` (a copy of `EXAMPLE_DIR/runtime.toml`) and
//! `EXAMPLE_DIR/application.toml`, on a fresh, empty store; the scenario
//! installs other manifests into `SCRATCH_DIR` and reloads the host.
//! `scripts/task-board-demo.sh` prepares all of this.
//!
//! Every step prints the request it sends and `ok: …` for each check; the
//! first failed check stops the run with exit status 1.

use std::cell::Cell;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Barrier};
use std::time::Duration;
use std::{fs, thread};

use serde_json::{Value, json};

type Check<T = ()> = Result<T, String>;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [address, scratch, example] = args.as_slice() else {
        eprintln!("usage: task-board-scenario <ADDRESS> <SCRATCH_DIR> <EXAMPLE_DIR>");
        return ExitCode::from(2);
    };
    let scenario = Scenario {
        address: address.clone(),
        scratch: PathBuf::from(scratch),
        example: PathBuf::from(example),
        messages: Cell::new(0),
    };
    match scenario.run() {
        Ok(()) => {
            println!("\n== Scenario passed");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("scenario: FAILED: {message}");
            ExitCode::FAILURE
        }
    }
}

/// One HTTP response.
#[derive(Debug, Clone, PartialEq)]
struct Response {
    status: u16,
    body: Value,
}

struct Scenario {
    address: String,
    scratch: PathBuf,
    example: PathBuf,
    messages: Cell<usize>,
}

/// A command as the scenario sends it.
#[derive(Clone)]
struct Command {
    tag: &'static str,
    stream: String,
    payload: Value,
    actor: Option<&'static str>,
    key: Option<&'static str>,
    expected_version: Option<u64>,
}

fn command(tag: &'static str, stream: &str, payload: Value) -> Command {
    Command {
        tag,
        stream: stream.to_owned(),
        payload,
        actor: Some("ada"),
        key: None,
        expected_version: None,
    }
}

impl Command {
    fn actor(mut self, actor: &'static str) -> Self {
        self.actor = Some(actor);
        self
    }

    fn key(mut self, key: &'static str) -> Self {
        self.key = Some(key);
        self
    }

    fn expect_version(mut self, version: u64) -> Self {
        self.expected_version = Some(version);
        self
    }

    fn body(&self, message: usize) -> Value {
        let mut metadata = json!({
            "message_id": format!("msg-{message}"),
            "correlation_id": "task-board-scenario",
        });
        if let Some(actor) = self.actor {
            metadata["actor"] = json!(actor);
        }
        let mut body = json!({
            "tag": self.tag,
            "stream": self.stream,
            "payload": self.payload,
            "metadata": metadata,
        });
        if let Some(key) = self.key {
            body["idempotency_key"] = json!(key);
        }
        if let Some(version) = self.expected_version {
            body["expected_version"] = json!(version);
        }
        body
    }
}

fn receipt(task: &str, version: u64, position: u64) -> Value {
    json!({ "stream": format!("Task/{task}"), "version": version, "position": position, "events": 1 })
}

fn error(code: &str, classification: &str) -> Value {
    json!({ "error": { "code": code, "classification": classification } })
}

fn rejected(code: &str) -> (u16, Value) {
    (422, error(code, "permanent"))
}

fn conflict(code: &str) -> (u16, Value) {
    (409, error(code, "conflict"))
}

fn ok(body: Value) -> (u16, Value) {
    (200, body)
}

fn step(title: &str) {
    println!("\n== {title}");
}

fn expect(response: &Response, (status, body): (u16, Value)) -> Check {
    if response.status == status && response.body == body {
        println!("ok: HTTP {status} {body}");
        Ok(())
    } else {
        Err(format!(
            "expected HTTP {status} {body}, got HTTP {} {}",
            response.status, response.body
        ))
    }
}

fn read_json(path: &Path) -> Check<Value> {
    let text =
        fs::read_to_string(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{} is not JSON: {e}", path.display()))
}

impl Scenario {
    fn run(&self) -> Check {
        let send = |command: &Command| self.command(command);

        step("The host serves the application next to the stateless services");
        let catalog = self.request("GET", "/v1/services", None)?;
        for service in ["task-domain", "task-view", "normalize", "greet"] {
            if !catalog.body["services"]
                .as_array()
                .is_some_and(|s| s.contains(&json!(service)))
            {
                return Err(format!("catalog lacks {service}: {}", catalog.body));
            }
        }
        println!("ok: catalog {}", catalog.body);
        self.welcome()?;

        step("Create three tasks through commands");
        let tasks = read_json(&self.example.join("fixtures/tasks.json"))?;
        let tasks = tasks
            .as_array()
            .ok_or("fixtures/tasks.json must be an array")?;
        for (index, task) in tasks.iter().enumerate() {
            let stream = task["stream"]
                .as_str()
                .ok_or("task fixture without stream")?;
            let create = command("CreateTask", stream, json!({ "title": task["title"] }));
            expect(&send(&create)?, ok(receipt(stream, 1, index as u64 + 1)))?;
        }

        step("Commands do not update the read model until it catches up");
        expect(
            &self.query("TaskSummary", json!({}))?,
            ok(json!({ "value": { "total": 0, "open": 0, "completed": 0 }, "checkpoint": 0 })),
        )?;
        let caught_up = self.projection("catch-up")?;
        let expected = read_json(&self.example.join("expected/catch-up-created.json"))?;
        expect(&caught_up, ok(expected))?;
        expect(
            &self.query("GetTask", json!({ "task_id": "Task/task-2" }))?,
            ok(json!({ "value": {
                "task_id": "Task/task-2", "title": "Review the release notes", "status": "open", "version": 1,
            }, "checkpoint": 3 })),
        )?;

        step("Valid transitions: rename an open task, complete another");
        let renamed = command(
            "RenameTask",
            "task-1",
            json!({ "title": "Write the user guide" }),
        );
        expect(&send(&renamed)?, ok(receipt("task-1", 2, 4)))?;
        expect(
            &send(&command("CompleteTask", "task-2", json!({})))?,
            ok(receipt("task-2", 2, 5)),
        )?;

        step("Invariants and validation reject commands without appending events");
        for (rejected_command, outcome) in [
            (
                command("CompleteTask", "task-2", json!({})),
                rejected("task_already_completed"),
            ),
            (
                command("RenameTask", "task-2", json!({ "title": "Too late" })),
                rejected("task_completed"),
            ),
            (
                command("CreateTask", "task-1", json!({ "title": "Again" })),
                rejected("task_exists"),
            ),
            (
                command("RenameTask", "task-9", json!({ "title": "Nobody" })),
                rejected("task_not_found"),
            ),
            // Replay of task-1's history knows the rename above.
            (
                command(
                    "RenameTask",
                    "task-1",
                    json!({ "title": " Write the user guide " }),
                ),
                rejected("title_unchanged"),
            ),
            (
                command("CreateTask", "task-8", json!({ "title": "   " })),
                rejected("invalid_title"),
            ),
            (
                command(
                    "CreateTask",
                    "task-8",
                    json!({ "title": "x", "owner": "ada" }),
                ),
                rejected("validation_failed"),
            ),
            (
                command("ArchiveTask", "task-1", json!({})),
                (404, error("handler_not_found", "permanent")),
            ),
        ] {
            expect(&send(&rejected_command)?, outcome)?;
        }

        step("Idempotency: same key replays, a changed payload is a mismatch, actors are isolated");
        let create_4 = command(
            "CreateTask",
            "task-4",
            json!({ "title": "Record the demo" }),
        )
        .key("create-task-4");
        // Position 6: none of the rejected commands appended an event.
        expect(&send(&create_4)?, ok(receipt("task-4", 1, 6)))?;
        expect(&send(&create_4)?, ok(receipt("task-4", 1, 6)))?;
        let canonical = command(
            "CreateTask",
            "task-4",
            json!({ "title": "  Record the demo" }),
        )
        .key("create-task-4");
        expect(&send(&canonical)?, ok(receipt("task-4", 1, 6)))?;
        let changed = command("CreateTask", "task-4", json!({ "title": "Record a demo" }))
            .key("create-task-4");
        expect(&send(&changed)?, conflict("idempotency_mismatch"))?;
        let other_actor = command("CreateTask", "task-5", json!({ "title": "Edit the video" }))
            .key("create-task-4")
            .actor("grace");
        expect(&send(&other_actor)?, ok(receipt("task-5", 1, 7)))?;

        step("Expected versions: a stale write fails, and its released key can be retried");
        let stale = command(
            "RenameTask",
            "task-3",
            json!({ "title": "Plan the live demo" }),
        )
        .key("rename-task-3")
        .expect_version(7);
        expect(&send(&stale)?, conflict("concurrency_conflict"))?;
        let current = stale.clone().expect_version(1);
        expect(&send(&current)?, ok(receipt("task-3", 2, 8)))?;
        expect(&send(&current)?, ok(receipt("task-3", 2, 8)))?;

        step("Concurrent writers on one stream: exactly one append wins");
        let writers: Vec<Command> = ["A", "B", "C", "D", "E", "F"]
            .iter()
            .map(|name| {
                command(
                    "RenameTask",
                    "task-3",
                    json!({ "title": format!("Contender {name}") }),
                )
                .expect_version(2)
            })
            .collect();
        let outcomes = self.concurrently(&writers)?;
        let winners: Vec<&Response> = outcomes.iter().filter(|r| r.status == 200).collect();
        let losers = outcomes
            .iter()
            .filter(|r| {
                **r == Response {
                    status: 409,
                    body: error("concurrency_conflict", "conflict"),
                }
            })
            .count();
        if winners.len() != 1
            || losers != writers.len() - 1
            || winners[0].body != receipt("task-3", 3, 9)
        {
            return Err(format!(
                "expected one receipt at version 3 and {} conflicts, got {outcomes:?}",
                writers.len() - 1
            ));
        }
        println!(
            "ok: one HTTP 200 {} and {losers} HTTP 409 concurrency_conflict",
            winners[0].body
        );

        step("Concurrent duplicates of one key: one execution, the rest replay or wait");
        let duplicate = command(
            "CreateTask",
            "task-6",
            json!({ "title": "Publish the post" }),
        )
        .key("create-task-6");
        let duplicates = vec![duplicate; 6];
        let outcomes = self.concurrently(&duplicates)?;
        let in_flight = Response {
            status: 409,
            body: error("idempotency_in_flight", "conflict"),
        };
        let replayed = Response {
            status: 200,
            body: receipt("task-6", 1, 10),
        };
        if !outcomes.contains(&replayed)
            || outcomes.iter().any(|r| *r != replayed && *r != in_flight)
        {
            return Err(format!(
                "expected receipts at position 10 or idempotency_in_flight, got {outcomes:?}"
            ));
        }
        let waits = outcomes.iter().filter(|r| **r == in_flight).count();
        println!(
            "ok: {} identical receipts {} and {waits} idempotency_in_flight",
            outcomes.len() - waits,
            replayed.body
        );

        step("Catch up and query the read model");
        let caught_up = self.projection("catch-up")?;
        if caught_up.status != 200 || caught_up.body["checkpoint"] != json!(10) {
            return Err(format!(
                "expected catch-up to position 10, got {caught_up:?}"
            ));
        }
        println!("ok: checkpoint 10");
        let winner = winning_title(&writers, &caught_up)?;
        expect(
            &self.query("TaskSummary", json!({}))?,
            ok(json!({ "value": { "total": 6, "open": 5, "completed": 1 }, "checkpoint": 10 })),
        )?;
        expect(
            &self.query("GetTask", json!({ "task_id": "Task/task-3" }))?,
            ok(json!({ "value": {
                "task_id": "Task/task-3", "title": winner, "status": "open", "version": 3,
            }, "checkpoint": 10 })),
        )?;
        let open = self.query("ListTasks", json!({ "status": "open", "limit": 2 }))?;
        expect(
            &open,
            ok(json!({ "value": {
                "tasks": [
                    { "task_id": "Task/task-1", "title": "Write the user guide", "status": "open", "version": 2 },
                    { "task_id": "Task/task-3", "title": winner, "status": "open", "version": 3 },
                ],
                "next": "Task/task-3",
            }, "checkpoint": 10 })),
        )?;
        expect(
            &self.query("GetTask", json!({ "task_id": "Task/task-9" }))?,
            rejected("task_not_found"),
        )?;
        expect(
            &self.query("ListTasks", json!({ "limit": 0 }))?,
            rejected("invalid_limit"),
        )?;

        step("A faulty view revision: catch-up fails and keeps its checkpoint");
        self.install_and_reload("runtime-faulty.toml", 2)?;
        let rename_4 = command(
            "RenameTask",
            "task-4",
            json!({ "title": "Record the final demo" }),
        );
        expect(&send(&rename_4)?, ok(receipt("task-4", 2, 11)))?;
        let failed = (429, error("projection_failed", "transient"));
        expect(&self.projection("catch-up")?, failed.clone())?;
        expect(
            &self.query("TaskSummary", json!({}))?,
            ok(json!({ "value": { "total": 6, "open": 5, "completed": 1 }, "checkpoint": 10 })),
        )?;
        expect(&self.projection("catch-up")?, failed)?;

        step("The fixed revision resumes from the checkpoint");
        self.install_and_reload("runtime.toml", 3)?;
        let resumed = self.projection("catch-up")?;
        if resumed.status != 200 || resumed.body["checkpoint"] != json!(11) {
            return Err(format!("expected catch-up to position 11, got {resumed:?}"));
        }
        println!("ok: checkpoint 11");
        expect(
            &self.query("GetTask", json!({ "task_id": "Task/task-4" }))?,
            ok(json!({ "value": {
                "task_id": "Task/task-4", "title": "Record the final demo", "status": "open", "version": 2,
            }, "checkpoint": 11 })),
        )?;

        step("Rebuild replays the whole history into the same state");
        let rebuilt = self.projection("rebuild")?;
        expect(&rebuilt, (resumed.status, resumed.body.clone()))?;
        // The concurrent rename's winner is not deterministic; the fixture
        // leaves its title out and the scenario fills it in.
        let mut expected = read_json(&self.example.join("expected/rebuild-final.json"))?;
        expected["state"]["tasks"]["Task/task-3"]["title"] = json!(winner);
        expect(&rebuilt, ok(expected))?;

        step("Queries never append: the next command continues at position 12");
        for _ in 0..3 {
            expect(
                &self.query("TaskSummary", json!({}))?,
                ok(json!({ "value": { "total": 6, "open": 5, "completed": 1 }, "checkpoint": 11 })),
            )?;
        }
        expect(
            &send(&command("CompleteTask", "task-1", json!({})))?,
            ok(receipt("task-1", 3, 12)),
        )?;

        step("The stateless welcome pipeline still answers as before");
        self.welcome()
    }

    fn welcome(&self) -> Check {
        let root = self.example.join("../welcome");
        let input =
            fs::read_to_string(root.join("fixtures/welcome.json")).map_err(|e| e.to_string())?;
        let expected = fs::read_to_string(root.join("expected/stage-1-welcome.json"))
            .map_err(|e| e.to_string())?;
        let (status, body) = self.raw(
            "POST",
            "/v1/pipelines/welcome/invoke",
            Some(input.trim_end()),
        )?;
        if status == 200 && body == expected.trim_end() {
            println!("ok: HTTP 200 {body}");
            Ok(())
        } else {
            Err(format!(
                "expected HTTP 200 {}, got HTTP {status} {body}",
                expected.trim_end()
            ))
        }
    }

    fn command(&self, command: &Command) -> Check<Response> {
        let body = command.body(self.next_message());
        self.request("POST", "/v1/application/commands", Some(&body))
    }

    /// A fresh message id number: every command is a distinct message.
    fn next_message(&self) -> usize {
        self.messages.set(self.messages.get() + 1);
        self.messages.get()
    }

    fn query(&self, tag: &str, payload: Value) -> Check<Response> {
        let body = json!({
            "tag": tag,
            "payload": payload,
            "metadata": { "message_id": "query", "correlation_id": "task-board-scenario", "actor": "ada" },
        });
        self.request("POST", "/v1/application/queries", Some(&body))
    }

    fn projection(&self, operation: &str) -> Check<Response> {
        self.request(
            "POST",
            &format!("/v1/application/projections/tasks/{operation}"),
            None,
        )
    }

    /// Sends `commands` at once, released together by a barrier.
    fn concurrently(&self, commands: &[Command]) -> Check<Vec<Response>> {
        let barrier = Arc::new(Barrier::new(commands.len()));
        let handles: Vec<_> = commands
            .iter()
            .map(|command| {
                let (barrier, address) = (Arc::clone(&barrier), self.address.clone());
                let body = command.body(self.next_message()).to_string();
                thread::spawn(move || {
                    barrier.wait();
                    send(&address, "POST", "/v1/application/commands", Some(&body))
                })
            })
            .collect();
        println!(
            "-> {} concurrent POST /v1/application/commands",
            commands.len()
        );
        handles
            .into_iter()
            .map(|handle| {
                let (status, body) = handle.join().map_err(|_| "a writer thread panicked")??;
                parse(status, &body)
            })
            .collect()
    }

    /// Atomically replaces the startup manifest and reloads the host.
    fn install_and_reload(&self, manifest: &str, generation: u64) -> Check {
        let staging = self.scratch.join("runtime.toml.next");
        fs::copy(self.example.join(manifest), &staging)
            .map_err(|e| format!("cannot copy {manifest}: {e}"))?;
        fs::rename(&staging, self.scratch.join("runtime.toml")).map_err(|e| e.to_string())?;
        expect(
            &self.request("POST", "/v1/reload", None)?,
            ok(json!({ "generation": generation })),
        )
    }

    fn request(&self, method: &str, path: &str, body: Option<&Value>) -> Check<Response> {
        let body = body.map(Value::to_string);
        let (status, text) = self.raw(method, path, body.as_deref())?;
        parse(status, &text)
    }

    fn raw(&self, method: &str, path: &str, body: Option<&str>) -> Check<(u16, String)> {
        match body {
            Some(body) => println!("-> {method} {path} {body}"),
            None => println!("-> {method} {path}"),
        }
        send(&self.address, method, path, body)
    }
}

/// The title of the one writer whose rename the read model shows.
fn winning_title(writers: &[Command], caught_up: &Response) -> Check<String> {
    let title = caught_up.body["state"]["tasks"]["Task/task-3"]["title"]
        .as_str()
        .ok_or_else(|| format!("task-3 missing from {}", caught_up.body))?;
    writers
        .iter()
        .any(|writer| writer.payload["title"] == title)
        .then(|| title.to_owned())
        .ok_or_else(|| format!("task-3's title {title:?} is not one of the concurrent writers'"))
}

fn parse(status: u16, body: &str) -> Check<Response> {
    let body = serde_json::from_str(body)
        .map_err(|e| format!("HTTP {status} body is not JSON ({e}): {body}"))?;
    Ok(Response { status, body })
}

/// One HTTP/1.1 exchange on a fresh connection. The host answers with a
/// `Content-Length` body; the connection closes after the response.
fn send(address: &str, method: &str, path: &str, body: Option<&str>) -> Check<(u16, String)> {
    let failed = |e: std::io::Error| format!("{method} {path}: {e}");
    let mut stream = TcpStream::connect(address).map_err(failed)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(failed)?;
    let body = body.unwrap_or("");
    let content_type = if body.is_empty() {
        ""
    } else {
        "Content-Type: application/json\r\n"
    };
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n{content_type}Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .map_err(failed)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(failed)?;
    let response = String::from_utf8(response)
        .map_err(|_| format!("{method} {path}: response is not UTF-8"))?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("{method} {path}: malformed response"))?;
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return Err(format!(
            "{method} {path}: chunked responses are not supported"
        ));
    }
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("{method} {path}: malformed status line"))?;
    Ok((status, body.to_owned()))
}
