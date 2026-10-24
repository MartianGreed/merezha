//! `merezha content diff --config merezha.toml` against real PostgreSQL.
use merezha_content::{
    ActivationOptions, CONTENT_PERMISSIONS, ContentModel, MIGRATIONS, ModelOptions, activate,
};
use merezha_jobs::{PgJobs, PgOutbox};
use merezha_platform::{
    ConfigInput, Database, OsRandom,
    ports::{Source, SourceOutbox},
};
use merezha_postgres::{MigrationMode, ModuleMigrations, TestDatabase, run_migrations};
use std::{
    path::Path,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

const V1: &str = r#"version = 1
locales = [{ code = "en", default = true }]

[types.page]
access = "public"
fields = [
  { name = "title", type = "string", required = true },
  { name = "body", type = "text" },
]
"#;

fn project(dir: &Path, model: &str) {
    std::fs::write(
        dir.join("merezha.toml"),
        "[app]\nid = \"site\"\n[database]\nurl = { env = \"CONTENT_DIFF_DATABASE_URL\" }\n\
         [runtime]\nmanifest = \"runtime.toml\"\n[modules.content]\n",
    )
    .unwrap();
    std::fs::write(dir.join("content.toml"), model).unwrap();
}

fn input(dir: &Path, url: &str) -> ConfigInput {
    ConfigInput {
        path: dir.join("merezha.toml"),
        env: vec![("CONTENT_DIFF_DATABASE_URL".into(), url.into())],
        overrides: Vec::new(),
    }
}

async fn activate_model(test: &TestDatabase, source: &str) {
    let db = Database::connect(&test.config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    run_migrations(
        &db,
        &[
            ModuleMigrations {
                module: "content",
                migrations: MIGRATIONS,
            },
            ModuleMigrations {
                module: "jobs",
                migrations: PgJobs::migrations(),
            },
        ],
        MigrationMode::Apply,
        deadline,
    )
    .await
    .unwrap();
    let options = ModelOptions {
        permissions: &CONTENT_PERMISSIONS,
        media: false,
        organizations: false,
    };
    let model = ContentModel::parse(source, &options).unwrap();
    let outbox = SourceOutbox::new(Source::new("content").unwrap(), Arc::new(PgOutbox));
    activate(
        &db,
        &outbox,
        &OsRandom,
        &model,
        &ActivationOptions::default(),
    )
    .await
    .unwrap();
    db.close();
}

#[tokio::test]
async fn the_diff_classifies_against_the_database_and_fails_on_refused_changes() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), V1);
    // Nothing migrated yet: the model would activate as version 1.
    let (text, code) = merezha_cli::content::diff(&input(dir.path(), test.url()))
        .await
        .unwrap();
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("active none\n"), "{text}");
    assert!(text.contains("status fresh"), "{text}");
    activate_model(&test, V1).await;
    let (text, code) = merezha_cli::content::diff(&input(dir.path(), test.url()))
        .await
        .unwrap();
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("status current\n"), "{text}");
    // An additive change with an index to build.
    let additive = V1.replace("version = 1", "version = 2").replace(
        "{ name = \"body\", type = \"text\" },",
        "{ name = \"body\", type = \"text\" },\n  { name = \"rank\", type = \"integer\", sortable = true },",
    );
    project(dir.path(), &additive);
    let (text, code) = merezha_cli::content::diff(&input(dir.path(), test.url()))
        .await
        .unwrap();
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("status pending"), "{text}");
    assert!(
        text.contains("additive types.page.fields.rank field_added\n"),
        "{text}"
    );
    assert_eq!(text.matches("index build content_ix_").count(), 2, "{text}");
    // A refused change exits 1, from the binary too.
    let refused = V1
        .replace("version = 1", "version = 2")
        .replace("  { name = \"body\", type = \"text\" },\n", "");
    project(dir.path(), &refused);
    let (text, code) = merezha_cli::content::diff(&input(dir.path(), test.url()))
        .await
        .unwrap();
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("status content_model_breaking"), "{text}");
    assert!(
        text.contains("refused types.page.fields.body field_removed\n"),
        "{text}"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["content", "diff", "--config"])
        .arg(dir.path().join("merezha.toml"))
        .env("CONTENT_DIFF_DATABASE_URL", test.url())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(String::from_utf8(output.stdout).unwrap(), text);
    // The same version with another fingerprint.
    project(
        dir.path(),
        &V1.replace("required = true", "required = false"),
    );
    let (text, code) = merezha_cli::content::diff(&input(dir.path(), test.url()))
        .await
        .unwrap();
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains("status content_model_version_unchanged"),
        "{text}"
    );
}

#[test]
fn an_invalid_model_or_a_missing_section_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), "version = 1\n[types.page]\n");
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["content", "diff", "--config"])
        .arg(dir.path().join("merezha.toml"))
        .env("CONTENT_DIFF_DATABASE_URL", "postgres://127.0.0.1:1/none")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("\"code\":\"invalid_config\""), "{stderr}");
    assert!(stderr.contains("modules.content.model:locales"), "{stderr}");
    std::fs::write(
        dir.path().join("merezha.toml"),
        "[app]\nid = \"site\"\n[database]\nurl = { env = \"CONTENT_DIFF_DATABASE_URL\" }\n\
         [runtime]\nmanifest = \"runtime.toml\"\n",
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["content", "diff", "--config"])
        .arg(dir.path().join("merezha.toml"))
        .env("CONTENT_DIFF_DATABASE_URL", "postgres://127.0.0.1:1/none")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("content_not_enabled")
    );
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .args(["content", "check"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}
