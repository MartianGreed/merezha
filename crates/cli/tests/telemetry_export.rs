//! `merezha telemetry export` as a process (TE §7, TE-T7): it writes the
//! embedded artifacts, reports them as JSON, replaces its own files on a
//! second run and fails cleanly where it cannot write.
use merezha_telemetry::artifacts::ARTIFACTS;
use serde_json::Value;
use std::{fs, path::Path, process::Command};

fn export(args: &[&str]) -> (Option<i32>, Value, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_merezha"))
        .arg("telemetry")
        .args(args)
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let report = serde_json::from_str(stdout.trim()).unwrap_or(Value::Null);
    (
        output.status.code(),
        report,
        String::from_utf8(output.stderr).unwrap(),
    )
}

fn out_arg(dir: &Path) -> &str {
    dir.to_str().unwrap()
}

#[test]
fn export_writes_the_dashboards_and_rules_this_host_embeds() {
    let temp = tempfile::tempdir().unwrap();
    let out = temp.path().join("ops/telemetry");
    let (code, report, stderr) = export(&["export", "--out", out_arg(&out)]);
    assert_eq!(code, Some(0), "{report} {stderr}");
    assert_eq!(report["status"], "exported");
    assert_eq!(report["host_version"], env!("CARGO_PKG_VERSION"));
    let files: Vec<&str> = report["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f.as_str().unwrap())
        .collect();
    assert_eq!(files, ARTIFACTS.iter().map(|a| a.path).collect::<Vec<_>>());
    for path in [
        "dashboards.yaml",
        "dashboards/merezha-overview.json",
        "rules/merezha-alerts.yaml",
        "rules/merezha-recording.yaml",
    ] {
        assert!(files.contains(&path), "{path}");
    }
    for artifact in ARTIFACTS {
        assert_eq!(
            fs::read_to_string(out.join(artifact.path)).unwrap(),
            artifact.contents,
            "{}",
            artifact.path
        );
    }
    // Byte for byte the repository's deploy/telemetry.
    let deploy = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/telemetry");
    for artifact in ARTIFACTS {
        assert_eq!(
            fs::read(out.join(artifact.path)).unwrap(),
            fs::read(deploy.join(artifact.path)).unwrap()
        );
    }
    assert!(stderr.is_empty(), "{stderr}");

    // A refresh after an upgrade: the export replaces its own files and
    // leaves the project's others alone.
    fs::write(
        out.join("lgtm.Dockerfile"),
        "FROM grafana/otel-lgtm:0.38.0\n",
    )
    .unwrap();
    fs::write(out.join("dashboards/merezha-overview.json"), "{}").unwrap();
    let (code, report, _) = export(&["export", "--out", out_arg(&out)]);
    assert_eq!(code, Some(0), "{report}");
    assert_eq!(
        fs::read_to_string(out.join("dashboards/merezha-overview.json")).unwrap(),
        ARTIFACTS[1].contents
    );
    assert_eq!(
        fs::read_to_string(out.join("lgtm.Dockerfile")).unwrap(),
        "FROM grafana/otel-lgtm:0.38.0\n"
    );
}

#[test]
fn export_refuses_bad_arguments_with_the_usage() {
    for args in [
        &["export"][..],
        &["export", "--out"][..],
        &["export", "--dir", "x"][..],
        &["export", "--out", "a", "--out", "b"][..],
        &["export", "--out", "--help"][..],
        &["import"][..],
        &[][..],
    ] {
        let (code, report, _) = export(args);
        assert_eq!(code, Some(2), "{args:?}");
        assert_eq!(report["status"], "invalid");
        assert_eq!(report["error"]["code"], "invalid_arguments");
        assert_eq!(report["usage"], "merezha telemetry export --out DIR");
    }
}

#[test]
fn export_reports_the_file_it_cannot_write() {
    let temp = tempfile::tempdir().unwrap();
    // `rules` is a file, so the rule files cannot be written below it.
    fs::write(temp.path().join("rules"), "").unwrap();
    let (code, report, _) = export(&["export", "--out", out_arg(temp.path())]);
    assert_eq!(code, Some(1), "{report}");
    assert_eq!(report["status"], "failed");
    assert_eq!(report["error"]["code"], "export_failed");
    assert_eq!(report["error"]["file"], "rules/merezha-recording.yaml");
    assert_eq!(report["error"]["reason"], "not_a_directory");
    let replaced: Vec<&str> = ARTIFACTS
        .iter()
        .map(|a| a.path)
        .take_while(|p| !p.starts_with("rules/"))
        .collect();
    assert_eq!(report["files"], serde_json::json!(replaced));
    // The export directory itself cannot be created below a file.
    let (code, report, _) = export(&[
        "export",
        "--out",
        out_arg(&temp.path().join("rules/telemetry")),
    ]);
    assert_eq!(code, Some(1), "{report}");
    assert_eq!(report["error"]["file"], Value::Null);
}
