//! Usage: `merezha-componentize <core-module.wasm> <component.wasm>`
//!
//! Writes the component only when it satisfies the service contract; see
//! `merezha_guest_build::componentize` for the checks. The same code runs
//! as `merezha componentize`.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let (Some(input), Some(output), None) = (args.next(), args.next(), args.next()) else {
        eprintln!(
            "merezha-componentize: usage: merezha-componentize <core-module.wasm> <component.wasm>"
        );
        return ExitCode::FAILURE;
    };
    match merezha_guest_build::componentize_file(&PathBuf::from(input), &PathBuf::from(output)) {
        Ok(_) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("merezha-componentize: {error}");
            ExitCode::FAILURE
        }
    }
}
