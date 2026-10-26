//! The registry the `merezha` binary runs with (TE §1.3 "PostgreSQL"):
//! the kernel's statements and every module's, which must not share a name,
//! since `merezha.db.statement` tells them apart on spans and series. The
//! lists come from the modules themselves ([`Module::statements`]): every
//! block [`merezha_cli::modules`] registers, the jobs runtime and the
//! application module `run` adds, so a new block is checked as soon as the
//! binary registers it.
use merezha_platform::{ConfigInput, Module, ProjectConfig};
use merezha_postgres::{KERNEL_STATEMENTS, Statement, check_registry};

/// Every module the binary can host, by name.
fn host_modules() -> Vec<(&'static str, Vec<&'static [Statement]>)> {
    let input = ConfigInput {
        path: "merezha.toml".into(),
        env: Vec::new(),
        overrides: Vec::new(),
    };
    let config = ProjectConfig::parse(
        "[app]\nid = \"registry\"\n[database]\nurl = { env = \"URL\" }\n\
         [runtime]\nmanifest = \"runtime.toml\"\n",
        ".".into(),
        &[("URL".into(), "postgres://localhost/registry".into())],
        &[],
    )
    .expect("a minimal configuration");
    let jobs = merezha_jobs::PgJobs::new(&config.jobs);
    let mut modules: Vec<(&'static str, Vec<&'static [Statement]>)> = merezha_cli::modules(&input)
        .iter()
        .map(|module| (module.name(), described(module.as_ref())))
        .collect();
    modules.push((jobs.name(), described(&jobs)));
    modules.push((
        merezha_application::postgres::MODULE,
        merezha_cli::application_module::STATEMENTS.to_vec(),
    ));
    modules
}

/// A module's statement lists; one with a schema must declare them.
fn described(module: &dyn Module) -> Vec<&'static [Statement]> {
    let statements = module.statements().to_vec();
    assert!(
        module.migrations().is_empty() || statements.iter().any(|set| !set.is_empty()),
        "module {} has migrations but lists no statements",
        module.name()
    );
    statements
}

#[test]
fn every_module_registers_beside_the_kernel_without_a_shared_name() {
    let modules = host_modules();
    let names: Vec<&str> = modules.iter().map(|(name, _)| *name).collect();
    // The blocks come from the binary's registry, not from this list.
    for block in [
        "identity",
        "content",
        "notifications",
        "jobs",
        "application",
    ] {
        assert!(names.contains(&block), "{block} missing from {names:?}");
    }
    let mut sets = vec![KERNEL_STATEMENTS];
    sets.extend(modules.iter().flat_map(|(_, sets)| sets.iter().copied()));
    assert_eq!(check_registry(&sets), Ok(()));
}

/// A block with a schema whose module lists no statements.
struct Unlisted;
impl Module for Unlisted {
    fn name(&self) -> &'static str {
        "unlisted"
    }
    fn migrations(&self) -> &[merezha_platform::Migration] {
        const MIGRATIONS: &[merezha_platform::Migration] = &[merezha_platform::Migration::new(
            1,
            "core",
            "CREATE SCHEMA unlisted",
        )];
        MIGRATIONS
    }
}

#[test]
#[should_panic(expected = "module unlisted has migrations but lists no statements")]
fn a_module_with_a_schema_must_list_its_statements() {
    described(&Unlisted);
}
