//! A throwaway project directory with `merezha.toml` and template files,
//! configured through the host exactly as `merezha run` does.
#![allow(dead_code)]
use merezha_notifications::{Checked, HostFacts, Notifications};
use merezha_platform::{ConfigErrors, ConfigInput, Module, host};
use std::{path::Path, sync::Arc};

pub const KERNEL: &str = r#"
[app]
id = "tasks"
name = "Tasks"
[database]
url = { env = "APP_DATABASE_URL" }
[runtime]
manifest = "runtime.toml"
"#;
/// base64 of 32 bytes.
pub const KEY: &str = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
pub const MINIMAL: &str = r#"
[modules.notifications]
secret_key = { env = "NOTIFY_KEY" }
"#;

pub struct Project {
    pub dir: tempfile::TempDir,
    pub env: Vec<(String, String)>,
}
impl Project {
    /// `text` is appended to the kernel sections.
    pub fn new(text: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("merezha.toml"), format!("{KERNEL}{text}")).unwrap();
        Self {
            dir,
            env: vec![
                (
                    "APP_DATABASE_URL".into(),
                    "postgres://app@127.0.0.1:1/app".into(),
                ),
                ("NOTIFY_KEY".into(), KEY.into()),
            ],
        }
    }
    /// Raw `merezha.toml` text, without the kernel sections.
    pub fn raw(text: &str) -> Self {
        let project = Self::new("");
        std::fs::write(project.dir.path().join("merezha.toml"), text).unwrap();
        project
    }
    pub fn env(mut self, name: &str, value: &str) -> Self {
        self.env.push((name.into(), value.into()));
        self
    }
    /// Write a file under `notifications/templates/`.
    pub fn template(self, path: &str, text: &str) -> Self {
        let full = self.dir.path().join("notifications/templates").join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, text).unwrap();
        self
    }
    pub fn path(&self) -> &Path {
        self.dir.path()
    }
    pub fn input(&self) -> ConfigInput {
        ConfigInput {
            path: self.dir.path().join("merezha.toml"),
            env: self.env.clone(),
            overrides: Vec::new(),
        }
    }
    /// Configure through `host::configure`, kernel and module together.
    pub fn configure(&self) -> Result<Arc<Checked>, ConfigErrors> {
        let input = self.input();
        let module = Notifications::new(HostFacts::load(&input));
        let handle = module.handle();
        let modules: Vec<Box<dyn Module>> = vec![Box::new(module)];
        host::configure(&input, modules)?;
        Ok(handle.checked().expect("the section is present"))
    }
    /// The issue codes of a failed configuration, as `(key, code)`.
    pub fn issues(&self) -> Vec<(String, &'static str)> {
        match self.configure() {
            Ok(_) => Vec::new(),
            Err(errors) => errors.0.iter().map(|i| (i.key.clone(), i.code)).collect(),
        }
    }
}

/// Whether `issues` holds `(key, code)`.
pub fn has(issues: &[(String, &'static str)], key: &str, code: &str) -> bool {
    issues.iter().any(|(k, c)| k == key && *c == code)
}
