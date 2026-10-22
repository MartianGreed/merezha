//! Usage: `task-board <COMMAND> [ARGS]`
//!
//! Sends task board commands and queries to a running Merezha host over its
//! HTTP application endpoints and prints the JSON answer. Every query first
//! catches the `tasks` read model up, so it reflects all accepted commands.
//! `scripts/task-board-local.sh` starts a host this client can talk to.
//!
//! ```text
//! create <TASK> <TITLE>   CreateTask on stream TASK
//! rename <TASK> <TITLE>   RenameTask
//! complete <TASK>         CompleteTask
//! get <TASK>              GetTask
//! list [open|completed]   ListTasks, optionally by status
//! summary                 TaskSummary
//! catch-up | rebuild      run that read-model operation alone
//! ```
//!
//! `TASK` is a stream name of ASCII letters, digits and `._-:`.
//! Environment: `MEREZHA_ADDRESS` (default `127.0.0.1:8787`) and
//! `MEREZHA_ACTOR` (default `local`), the actor recorded on each message.
//! Exit status 1 means the host rejected the request or was unreachable;
//! its JSON error then goes to stderr. Exit status 2 means bad usage.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

const USAGE: &str = "usage: task-board create <TASK> <TITLE> | rename <TASK> <TITLE> | complete <TASK> \
| get <TASK> | list [open|completed] | summary | catch-up | rebuild";

/// One request the client sends.
enum Request {
    Command {
        tag: &'static str,
        task: String,
        payload: Value,
    },
    Query {
        tag: &'static str,
        payload: Value,
    },
    Projection(&'static str),
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(request) = parse_args(&args) else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let client = Client {
        address: std::env::var("MEREZHA_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8787".to_owned()),
        actor: std::env::var("MEREZHA_ACTOR").unwrap_or_else(|_| "local".to_owned()),
    };
    match client.send(&request) {
        Ok(body) => {
            println!("{}", pretty(&body));
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args(args: &[String]) -> Option<Request> {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let command = |tag, task: &str, payload| Request::Command {
        tag,
        task: task.to_owned(),
        payload,
    };
    let query = |tag, payload| Request::Query { tag, payload };
    Some(match args.as_slice() {
        ["create", task, title] => command("CreateTask", task, json!({ "title": title })),
        ["rename", task, title] => command("RenameTask", task, json!({ "title": title })),
        ["complete", task] => command("CompleteTask", task, json!({})),
        ["get", task] => query("GetTask", json!({ "task_id": format!("Task/{task}") })),
        ["list"] => query("ListTasks", json!({})),
        ["list", status @ ("open" | "completed")] => {
            query("ListTasks", json!({ "status": status }))
        }
        ["summary"] => query("TaskSummary", json!({})),
        ["catch-up"] => Request::Projection("catch-up"),
        ["rebuild"] => Request::Projection("rebuild"),
        _ => return None,
    })
}

struct Client {
    address: String,
    actor: String,
}

impl Client {
    fn send(&self, request: &Request) -> Result<Value, String> {
        match request {
            Request::Command { tag, task, payload } => self.post(
                "/v1/application/commands",
                Some(json!({ "tag": tag, "stream": task, "payload": payload, "metadata": self.metadata() })),
            ),
            Request::Query { tag, payload } => {
                self.post("/v1/application/projections/tasks/catch-up", None)?;
                self.post(
                    "/v1/application/queries",
                    Some(json!({ "tag": tag, "payload": payload, "metadata": self.metadata() })),
                )
            }
            Request::Projection(operation) => {
                self.post(&format!("/v1/application/projections/tasks/{operation}"), None)
            }
        }
    }

    /// Message metadata with an identifier unique to this process and call.
    fn metadata(&self) -> Value {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        json!({
            "message_id": format!("cli-{}-{nanos}", std::process::id()),
            "correlation_id": "task-board-cli",
            "actor": self.actor,
        })
    }

    fn post(&self, path: &str, body: Option<Value>) -> Result<Value, String> {
        let body = body.map(|body| body.to_string()).unwrap_or_default();
        let (status, text) = exchange(&self.address, path, &body)?;
        let answer: Value = serde_json::from_str(&text)
            .map_err(|e| format!("POST {path}: HTTP {status} body is not JSON ({e}): {text}"))?;
        if status == 200 {
            Ok(answer)
        } else {
            Err(format!("HTTP {status}\n{}", pretty(&answer)))
        }
    }
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

/// One HTTP/1.1 POST on a fresh connection. The host answers with a
/// `Content-Length` body and closes the connection after the response.
fn exchange(address: &str, path: &str, body: &str) -> Result<(u16, String), String> {
    let failed = |e: std::io::Error| format!("POST {path} to {address}: {e}");
    let mut stream = TcpStream::connect(address).map_err(failed)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(failed)?;
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .map_err(failed)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(failed)?;
    let response =
        String::from_utf8(response).map_err(|_| format!("POST {path}: response is not UTF-8"))?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("POST {path}: malformed response"))?;
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        return Err(format!("POST {path}: chunked responses are not supported"));
    }
    let status = head
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("POST {path}: malformed status line"))?;
    Ok((status, body.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| (*word).to_owned()).collect()
    }

    #[test]
    fn commands_target_the_task_stream() {
        let Some(Request::Command { tag, task, payload }) =
            parse_args(&args(&["rename", "groceries", "Buy oat milk"]))
        else {
            panic!("rename parses as a command");
        };
        assert_eq!((tag, task.as_str()), ("RenameTask", "groceries"));
        assert_eq!(payload, json!({ "title": "Buy oat milk" }));
    }

    #[test]
    fn get_names_the_aggregate_stream() {
        let Some(Request::Query { tag, payload }) = parse_args(&args(&["get", "groceries"])) else {
            panic!("get parses as a query");
        };
        assert_eq!(tag, "GetTask");
        assert_eq!(payload, json!({ "task_id": "Task/groceries" }));
    }

    #[test]
    fn list_filters_only_by_known_status() {
        let Some(Request::Query { payload, .. }) = parse_args(&args(&["list", "open"])) else {
            panic!("list open parses as a query");
        };
        assert_eq!(payload, json!({ "status": "open" }));
        assert!(parse_args(&args(&["list", "closed"])).is_none());
    }

    #[test]
    fn rejects_missing_and_extra_arguments() {
        for words in [
            &[][..],
            &["create", "groceries"],
            &["complete", "a", "b"],
            &["delete", "a"],
        ] {
            assert!(parse_args(&args(words)).is_none(), "{words:?}");
        }
    }
}
