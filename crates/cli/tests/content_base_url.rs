//! Issue #46: `merezha run` tells the content block whether its listener
//! is private, the only place `delivery.public_base_url` may be plain
//! HTTP (to a loopback host).
use merezha_content::ContentModule;
use merezha_platform::{ConfigInput, host::configure};
use std::path::Path;

const MODEL: &str = "version = 1\nlocales = [{ code = \"en\", default = true }]\n\
                     [types.page]\naccess = \"public\"\n\
                     fields = [{ name = \"title\", type = \"string\" }]\n";

/// The `delivery.public_base_url` codes configuring the binary's blocks
/// reports for `base` under `[http]` with `http`; empty when accepted.
fn issues(dir: &Path, http: &str, base: &str) -> Vec<String> {
    std::fs::write(dir.join("content.toml"), MODEL).unwrap();
    std::fs::write(
        dir.join("merezha.toml"),
        format!(
            "[app]\nid = \"site\"\n[database]\nurl = {{ env = \"BASE_URL_TEST_DATABASE_URL\" }}\n\
             [runtime]\nmanifest = \"runtime.toml\"\n[http]\n{http}\n\
             [modules.content]\ndelivery = {{ public_base_url = \"{base}\" }}\n"
        ),
    )
    .unwrap();
    let input = ConfigInput {
        path: dir.join("merezha.toml"),
        env: vec![(
            "BASE_URL_TEST_DATABASE_URL".into(),
            "postgres://localhost/x".into(),
        )],
        overrides: Vec::new(),
    };
    match configure(&input, merezha_cli::blocks(&input, ContentModule::new())) {
        Ok(_) => Vec::new(),
        Err(errors) => errors
            .0
            .iter()
            .map(|issue| format!("{} {}", issue.key, issue.code))
            .collect(),
    }
}

#[test]
fn plain_http_base_urls_need_a_private_listener() {
    let dir = tempfile::tempdir().unwrap();
    let refused = vec!["modules.content.delivery.public_base_url insecure_origin".to_owned()];
    // A private listener: loopback, not public, no trusted proxy.
    assert_eq!(
        issues(
            dir.path(),
            "listen = \"127.0.0.1:8080\"",
            "http://localhost:8080"
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        issues(
            dir.path(),
            "listen = \"127.0.0.1:8080\"",
            "http://cms.example.com"
        ),
        refused
    );
    // A public listener, or one behind a trusted proxy: `https` only.
    for http in [
        "listen = \"0.0.0.0:8080\"\npublic = true",
        "listen = \"127.0.0.1:8080\"\npublic = true",
        "listen = \"127.0.0.1:8080\"\ntrusted_proxies = [\"10.0.0.0/8\"]",
    ] {
        assert_eq!(
            issues(dir.path(), http, "http://localhost:8080"),
            refused,
            "{http}"
        );
        assert_eq!(
            issues(dir.path(), http, "https://cms.example.com"),
            Vec::<String>::new(),
            "{http}"
        );
    }
}
