//! Usage: `merezha-componentize <core-module.wasm> <component.wasm>`
//!
//! Writes the component only when it satisfies the service contract; see
//! the library documentation for the checks.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail, ensure};
use merezha_componentize::{MAX_COMPONENT_BYTES, componentize};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("merezha-componentize: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let (Some(input), Some(output), None) = (args.next(), args.next(), args.next()) else {
        bail!("usage: merezha-componentize <core-module.wasm> <component.wasm>");
    };
    let (input, output) = (PathBuf::from(input), PathBuf::from(output));

    let module = read_bounded(&input)?;
    let component = componentize(&module).with_context(|| format!("{}", input.display()))?;
    write_atomically(&output, &component)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let size = fs::metadata(path)
        .with_context(|| format!("cannot read {}", path.display()))?
        .len();
    ensure!(
        size <= MAX_COMPONENT_BYTES as u64,
        "{} is {size} bytes; at most {MAX_COMPONENT_BYTES} are accepted",
        path.display()
    );
    fs::read(path).with_context(|| format!("cannot read {}", path.display()))
}

fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut staging = path.as_os_str().to_owned();
    staging.push(".partial");
    let staging = PathBuf::from(staging);
    fs::write(&staging, bytes).with_context(|| format!("cannot write {}", staging.display()))?;
    fs::rename(&staging, path).with_context(|| format!("cannot move {} into place", path.display()))
}
