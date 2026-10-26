//! The registry the `merezha` binary runs with (TE §1.3 "PostgreSQL"):
//! the kernel's statements and every module's, which must not share a name,
//! since `merezha.db.statement` tells them apart on spans and series. The
//! lists come from the modules themselves ([`Module::statements`]): every
//! block [`merezha_cli::modules`] registers, the jobs runtime and the
//! application module `run` adds, so a new block is checked as soon as the
//! binary registers it. Every statement constant a crate declares must be
//! in its module's lists (the kernel's in `KERNEL_STATEMENTS`), and a crate
//! that is no module declares none.
use merezha_application::{Guest, GuestSession};
use merezha_domain::{Failure, Operation, Reply};
use merezha_platform::{ConfigInput, Module, ProjectConfig, ports::LoopbackAllowAll};
use merezha_postgres::{KERNEL_STATEMENTS, Statement, check_registry};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};
use syn::{Expr, Item, Lit, Type, visit::Visit};

/// A guest that is never called: the module is only asked its lists.
struct Idle;
impl Guest for Idle {
    fn session(&self) -> Result<Box<dyn GuestSession>, Failure> {
        Ok(Box::new(Idle))
    }
    fn services(&self) -> Result<Vec<String>, Failure> {
        Ok(Vec::new())
    }
}
impl GuestSession for Idle {
    fn invoke(&self, _: &str, _: Operation) -> Result<Reply, Failure> {
        Err(Failure::permanent("idle"))
    }
}

/// Every module the binary can host, by name, with its statement lists.
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
    let application = merezha_cli::application_module::ApplicationModule::new(
        merezha_application::config::Config::parse(
            "version = 1\n[storage]\nbackend = \"postgres\"\napplication = \"registry\"\n",
        )
        .expect("a minimal application"),
        Arc::new(Idle),
        Arc::new(LoopbackAllowAll::new("127.0.0.1:8080".parse().unwrap()).unwrap()),
        merezha_application::postgres::PostgresOptions::default(),
        0,
    );
    let mut modules: Vec<(&'static str, Vec<&'static [Statement]>)> = merezha_cli::modules(&input)
        .iter()
        .map(|module| (module.name(), described(module.as_ref())))
        .collect();
    modules.push((jobs.name(), described(&jobs)));
    modules.push((application.name(), described(&application)));
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

fn crates_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/cli sits in crates/")
        .to_path_buf()
}

/// Whether `attrs` hold `#[cfg(test)]` or `#[test]`.
fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("test")
            || (attr.path().is_ident("cfg")
                && attr
                    .parse_args::<syn::Ident>()
                    .is_ok_and(|ident| ident == "test"))
    })
}

/// The names of the statement constants (`const X: Statement = …`) the
/// non-test code of `file` declares, from the first argument of their
/// initializer (`Statement::new("a.b", …)` or a block's `const fn` helper
/// taking the name first); modules it declares in other files are walked
/// from `dir`.
fn declared_in(path: &Path, dir: &Path, names: &mut BTreeMap<String, PathBuf>) {
    let source =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let file = syn::parse_file(&source)
        .unwrap_or_else(|e| panic!("{}: does not parse: {e}", path.display()));
    declared_items(&file.items, path, dir, names);
}

fn declared_items(items: &[Item], path: &Path, dir: &Path, names: &mut BTreeMap<String, PathBuf>) {
    struct Constants<'a> {
        path: &'a Path,
        names: &'a mut BTreeMap<String, PathBuf>,
    }
    impl Visit<'_> for Constants<'_> {
        fn visit_item_mod(&mut self, _: &syn::ItemMod) {
            // Walked by `declared_items`, which knows their directory.
        }
        fn visit_item_fn(&mut self, item: &syn::ItemFn) {
            if !test_only(&item.attrs) {
                syn::visit::visit_item_fn(self, item);
            }
        }
        fn visit_item_const(&mut self, item: &syn::ItemConst) {
            if test_only(&item.attrs) {
                return;
            }
            let statement = matches!(&*item.ty, Type::Path(p)
                if p.path.segments.last().is_some_and(|s| s.ident == "Statement"));
            if statement {
                let name = match &*item.expr {
                    Expr::Call(call) => match call.args.first() {
                        Some(Expr::Lit(lit)) => match &lit.lit {
                            Lit::Str(name) => Some(name.value()),
                            _ => None,
                        },
                        _ => None,
                    },
                    _ => None,
                };
                let name = name.unwrap_or_else(|| {
                    panic!(
                        "{}: statement {} takes its name from no literal",
                        self.path.display(),
                        item.ident
                    )
                });
                self.names.insert(name, self.path.to_path_buf());
            }
            syn::visit::visit_item_const(self, item);
        }
    }
    let mut constants = Constants { path, names };
    for item in items {
        match item {
            Item::Mod(module) if test_only(&module.attrs) => {}
            Item::Mod(module) => {
                let child = dir.join(module.ident.to_string());
                match &module.content {
                    Some((_, items)) => declared_items(items, path, &child, constants.names),
                    None => {
                        let file = dir.join(format!("{}.rs", module.ident));
                        let (file, below) = if file.is_file() {
                            (file, child)
                        } else {
                            (child.join("mod.rs"), child)
                        };
                        declared_in(&file, &below, constants.names);
                    }
                }
            }
            item => constants.visit_item(item),
        }
    }
}

/// The statement names each workspace crate's non-test code declares, by
/// crate directory.
fn declared() -> BTreeMap<String, BTreeMap<String, PathBuf>> {
    let mut crates = BTreeMap::new();
    for entry in std::fs::read_dir(crates_dir()).expect("crates/") {
        let member = entry.expect("directory entry").path();
        if !member.join("Cargo.toml").is_file() {
            continue;
        }
        let name = member
            .file_name()
            .and_then(|n| n.to_str())
            .expect("crate directory name")
            .to_owned();
        let src = member.join("src");
        let mut names = BTreeMap::new();
        for root in ["lib.rs", "main.rs"] {
            if src.join(root).is_file() {
                declared_in(&src.join(root), &src, &mut names);
            }
        }
        crates.insert(name, names);
    }
    crates
}

/// Every statement a crate declares is reachable from its module's lists,
/// so a list the module leaves out, or a module that lists none, fails.
#[test]
fn every_declared_statement_is_in_its_modules_lists() {
    let mut registered: BTreeMap<String, BTreeSet<&str>> = host_modules()
        .into_iter()
        .map(|(module, sets)| {
            let names = sets.iter().flat_map(|set| set.iter().map(|s| s.name));
            (module.to_owned(), names.collect())
        })
        .collect();
    registered.insert(
        "postgres".to_owned(),
        KERNEL_STATEMENTS.iter().map(|s| s.name).collect(),
    );
    let declared = declared();
    let mut errors = Vec::new();
    for (krate, names) in &declared {
        // A block's crate directory is its module's name.
        let listed = registered.get(krate.as_str());
        for (name, path) in names {
            if !listed.is_some_and(|listed| listed.contains(name.as_str())) {
                errors.push(format!(
                    "{name} ({}) is in no list of module {krate}",
                    path.display()
                ));
            }
        }
    }
    for module in registered.keys() {
        assert!(
            declared.get(module).is_some_and(|names| !names.is_empty()),
            "module {module} has no crate crates/{module} declaring its statements"
        );
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}
