//! The dependency rule (TE §0): libraries depend only on the APIs. A crate
//! built with `merezha-telemetry` default features links no SDK and no
//! exporter; `propagation` adds the span bridge but still no SDK; only the
//! `merezha` binary links the OTLP exporters.
use std::{path::Path, process::Command};

fn tree(args: &[&str]) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--locked",
            "--offline",
            "-e",
            "normal",
            "--prefix",
            "none",
        ])
        .args(["--format", "{p}"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("cargo tree runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
fn links(tree: &str, krate: &str) -> bool {
    tree.lines()
        .any(|line| line.starts_with(&format!("{krate} v")))
}
const SDK_AND_EXPORTERS: [&str; 5] = [
    "opentelemetry_sdk",
    "opentelemetry-otlp",
    "opentelemetry-proto",
    "tonic",
    "tracing-appender",
];

#[test]
fn libraries_with_default_features_link_no_sdk_or_exporter() {
    for package in [
        "merezha-telemetry",
        "merezha-platform",
        "merezha-runtime",
        "merezha-application",
        "merezha-postgres",
        "merezha-domain",
    ] {
        let tree = tree(&["-p", package]);
        for krate in SDK_AND_EXPORTERS {
            assert!(!links(&tree, krate), "{package} links {krate}");
        }
    }
    let propagation = tree(&["-p", "merezha-telemetry", "--features", "propagation"]);
    assert!(links(&propagation, "tracing-opentelemetry"));
    for krate in SDK_AND_EXPORTERS {
        assert!(!links(&propagation, krate), "propagation links {krate}");
    }
    let binary = tree(&["-p", "merezha-cli"]);
    for krate in ["opentelemetry_sdk", "opentelemetry-otlp", "tonic"] {
        assert!(links(&binary, krate), "the binary should link {krate}");
    }
}

/// T3: `merezha-runtime` gains no dependency beyond `tracing`, the
/// `opentelemetry` API and `merezha-telemetry` default features.
#[test]
fn the_runtime_depends_only_on_the_telemetry_apis() {
    let direct = tree(&["-p", "merezha-runtime", "--depth", "1"]);
    let mut names: Vec<&str> = direct
        .lines()
        .skip(1)
        .filter_map(|line| line.split(" v").next())
        .collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(
        names,
        [
            "merezha-telemetry",
            "opentelemetry",
            "serde",
            "toml",
            "tracing",
            "wasmparser",
            "wasmtime"
        ]
    );
    let full = tree(&["-p", "merezha-runtime"]);
    for krate in ["tracing-opentelemetry", "tracing-subscriber", "reqwest"] {
        assert!(!links(&full, krate), "the runtime links {krate}");
    }
}
