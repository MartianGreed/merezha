//! Template registry behaviour (NT §4): built-in templates against golden
//! files, strict undefined, escaping by extension, control characters,
//! locale fallback, filters and the startup checks on project files.
mod support;
use merezha_notifications::{
    Checked,
    templates::{Encoding, PREVIEW_NOTIFICATION_ID, RenderContext, Rendered},
    topics::Category,
};
use merezha_platform::ports::Channel;
use serde_json::{Map, Value, json};
use std::path::PathBuf;
use support::*;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

/// Render `template` like a send would, with `data` and no secret.
fn render(
    checked: &Checked,
    template: &str,
    channel: Channel,
    locale: &str,
    data: &Map<String, Value>,
) -> Result<Rendered, &'static str> {
    let empty = Map::new();
    let context = RenderContext {
        topic: template,
        category: Category::Transactional,
        notification_id: PREVIEW_NOTIFICATION_ID,
        data,
        secret: &empty,
        recipient_name: Some("Ada"),
        timezone: Some("Europe/Paris"),
        unsubscribe: None,
    };
    checked
        .registry
        .render(template, channel, locale, &context)
        .map_err(|e| e.code)
}

fn locales(list: &str) -> Project {
    Project::new(&format!(
        "{MINIMAL}\n[modules.notifications.templates]\nlocales = [{list}]\n"
    ))
}

/// One project topic with a template under `tasks.assigned`.
fn project_topic(channels: &str) -> String {
    format!(
        "{MINIMAL}\n[modules.notifications.topics.\"tasks.assigned\"]\ncategory = \"transactional\"\nchannels = [{channels}]\n"
    )
}

#[test]
fn builtin_templates_render_in_en_and_fr_as_the_golden_files() {
    let checked = locales("\"en\", \"fr\"").configure().unwrap();
    let golden = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let update = std::env::var("MEREZHA_UPDATE_GOLDEN").as_deref() == Ok("1");
    let mut seen = 0;
    for info in checked.registry.templates() {
        assert!(info.name.starts_with("identity.") || info.name.starts_with("notifications."));
        for locale in ["en", "fr"] {
            assert!(
                info.locales().contains(&locale.to_owned()),
                "{} has no {locale} files",
                info.name
            );
            let mut out = Map::new();
            for channel in info.channels(&checked.registry, locale) {
                let topic = &checked.settings.topics[&info.name];
                let unsubscribe = checked
                    .registry
                    .preview_unsubscribe(topic.category, channel);
                let context = RenderContext {
                    topic: &topic.name,
                    category: topic.category,
                    notification_id: PREVIEW_NOTIFICATION_ID,
                    data: &info.preview,
                    secret: &info.preview_secret,
                    recipient_name: Some("Ada Lovelace"),
                    timezone: Some("Europe/Paris"),
                    unsubscribe: unsubscribe.as_deref(),
                };
                let rendered = checked
                    .registry
                    .render(&info.name, channel, locale, &context)
                    .unwrap();
                out.insert(channel.as_str().into(), rendered.to_json());
            }
            let text = serde_json::to_string_pretty(&Value::Object(out)).unwrap() + "\n";
            let path = golden.join(format!("{}.{locale}.json", info.name));
            if update {
                std::fs::create_dir_all(&golden).unwrap();
                std::fs::write(&path, &text).unwrap();
            }
            let expected = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("missing golden file {}", path.display()));
            assert_eq!(text, expected, "{} drifted", path.display());
            seen += 1;
        }
    }
    assert_eq!(seen, 22, "eleven built-in templates in two locales");
}

#[test]
fn strict_undefined_fails_a_preview_at_startup() {
    let project = Project::new(&project_topic("\"email\""))
        .template("tasks.assigned/en/subject.txt", "Task {{ title }}")
        .template("tasks.assigned/en/body.txt", "Hi {{ missing }}")
        .template(
            "tasks.assigned/template.toml",
            "[preview]\ntitle = \"Write the report\"\n",
        );
    let issues = project.issues();
    assert!(
        has(
            &issues,
            "modules.notifications.templates.\"tasks.assigned\".preview.en.email",
            "undefined_variable"
        ),
        "{issues:?}"
    );
    // The same template renders once the preview supplies the variable.
    let fixed = Project::new(&project_topic("\"email\""))
        .template("tasks.assigned/en/subject.txt", "Task {{ title }}")
        .template("tasks.assigned/en/body.txt", "Hi {{ missing }}")
        .template(
            "tasks.assigned/template.toml",
            "[preview]\ntitle = \"Write the report\"\nmissing = \"there\"\n",
        );
    let checked = fixed.configure().unwrap();
    let data = object(json!({"title": "x"}));
    assert_eq!(
        render(&checked, "tasks.assigned", Channel::Email, "en", &data),
        Err("undefined_variable")
    );
}

#[test]
fn name_is_escaped_in_html_and_not_in_text() {
    let checked = Project::new(&project_topic("\"email\""))
        .template("tasks.assigned/en/subject.txt", "For {{ name }}")
        .template("tasks.assigned/en/body.txt", "Hello {{ name }}")
        .template("tasks.assigned/en/body.html", "<p>Hello {{ name }}</p>")
        .template(
            "tasks.assigned/template.toml",
            "required = [\"name\"]\n[preview]\nname = \"Ada\"\n",
        )
        .configure()
        .unwrap();
    let data = object(json!({"name": "<b>Ada & \"Bob\"</b>"}));
    let Ok(Rendered::Email(email)) =
        render(&checked, "tasks.assigned", Channel::Email, "en", &data)
    else {
        panic!("email renders");
    };
    assert_eq!(email.text, "Hello <b>Ada & \"Bob\"</b>");
    assert_eq!(email.subject, "For <b>Ada & \"Bob\"</b>");
    assert_eq!(
        email.html.as_deref(),
        Some("<p>Hello &lt;b&gt;Ada &amp; &quot;Bob&quot;&lt;&#x2f;b&gt;</p>")
    );
}

#[test]
fn a_control_character_in_a_subject_is_rejected() {
    let project = || {
        Project::new(&project_topic("\"email\", \"sms\"").replace(
            "[modules.notifications.topics",
            "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\n\n[modules.notifications.topics",
        ))
        .template("tasks.assigned/en/subject.txt", "{{ subject }}")
        .template("tasks.assigned/en/body.txt", "Body")
        .template("tasks.assigned/en/sms.txt", "{{ subject }}")
    };
    let checked = project()
        .template(
            "tasks.assigned/template.toml",
            "[preview]\nsubject = \"Fine\"\n",
        )
        .configure()
        .unwrap();
    for bad in [
        "Hi\rthere",
        "Hi\nthere",
        "Hi\u{7}",
        "Hi\u{2028}there",
        "Hi\u{202e}ereht",
    ] {
        let data = object(json!({"subject": bad}));
        assert_eq!(
            render(&checked, "tasks.assigned", Channel::Email, "en", &data),
            Err("control_character"),
            "{bad:?}"
        );
    }
    // SMS may contain a line feed, nothing else.
    let data = object(json!({"subject": "Hi\nthere"}));
    assert!(matches!(
        render(&checked, "tasks.assigned", Channel::Sms, "en", &data),
        Ok(Rendered::Sms(_))
    ));
    let data = object(json!({"subject": "Hi\rthere"}));
    assert_eq!(
        render(&checked, "tasks.assigned", Channel::Sms, "en", &data),
        Err("control_character")
    );
    // A preview with a CR stops startup.
    let issues = project()
        .template(
            "tasks.assigned/template.toml",
            "[preview]\nsubject = \"Line\\r\\nInjected: header\"\n",
        )
        .issues();
    assert!(
        has(
            &issues,
            "modules.notifications.templates.\"tasks.assigned\".preview.en.email",
            "control_character"
        ),
        "{issues:?}"
    );
}

#[test]
fn fr_ca_falls_back_to_fr_then_en() {
    let checked = locales("\"en\", \"fr\"").configure().unwrap();
    let registry = &checked.registry;
    assert_eq!(registry.resolve_locale(["fr-CA"]), "fr");
    assert_eq!(registry.resolve_locale(["de-DE", "fr-CA"]), "fr");
    assert_eq!(registry.resolve_locale(["de-DE"]), "en");
    let en_only = Project::new(MINIMAL).configure().unwrap();
    assert_eq!(en_only.registry.resolve_locale(["fr-CA"]), "en");
    assert_eq!(en_only.registry.resolve_locale(["fr"]), "en");

    // With `fr-CA` listed, its missing files fall back per channel: the
    // email to `fr`, the SMS (no French file) to `en`.
    let checked = Project::new(
        &project_topic("\"email\", \"sms\"")
            .replace(
                "[modules.notifications.topics",
                "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\n\n[modules.notifications.templates]\nlocales = [\"en\", \"fr\", \"fr-CA\"]\n\n[modules.notifications.topics",
            ),
    )
    .template("tasks.assigned/en/subject.txt", "Assigned")
    .template("tasks.assigned/en/body.txt", "You have a task")
    .template("tasks.assigned/en/sms.txt", "You have a task")
    .template("tasks.assigned/fr/subject.txt", "Assignée")
    .template("tasks.assigned/fr/body.txt", "Vous avez une tâche")
    .configure()
    .unwrap();
    let locale = checked.registry.resolve_locale(["fr-CA"]);
    assert_eq!(locale, "fr-CA");
    let data = Map::new();
    let Ok(Rendered::Email(email)) =
        render(&checked, "tasks.assigned", Channel::Email, &locale, &data)
    else {
        panic!("email renders");
    };
    assert_eq!(email.subject, "Assignée");
    let Ok(Rendered::Sms(sms)) = render(&checked, "tasks.assigned", Channel::Sms, &locale, &data)
    else {
        panic!("sms renders");
    };
    assert_eq!(sms.text, "You have a task");
}

#[test]
fn filters_check_links_and_format_dates_in_the_recipient_timezone() {
    let project = |allow_http: bool| {
        Project::new(&project_topic("\"email\"").replace(
            "[modules.notifications.topics",
            &format!("[modules.notifications.templates]\nallow_http_links = {allow_http}\n\n[modules.notifications.topics"),
        ))
        .template("tasks.assigned/en/subject.txt", "{{ title | truncate_chars(5) }}")
        .template(
            "tasks.assigned/en/body.txt",
            "{{ link | url }} {{ at | datetime(\"%Y-%m-%d %H:%M %Z\") }}",
        )
        .template(
            "tasks.assigned/template.toml",
            "[preview]\ntitle = \"Héllo wörld\"\nlink = \"https://app.example.com/t/1\"\nat = \"2026-03-29T00:30:00Z\"\n",
        )
    };
    let checked = project(false).configure().unwrap();
    let data =
        |link: &str| object(json!({"title": "Héllo wörld", "link": link, "at": 1_774_747_800}));
    let Ok(Rendered::Email(email)) = render(
        &checked,
        "tasks.assigned",
        Channel::Email,
        "en",
        &data("https://a.example/x?y=1&z=2"),
    ) else {
        panic!("email renders");
    };
    assert_eq!(email.subject, "Héllo");
    // 2026-03-29 01:30 UTC is just after the DST switch in Paris (CEST, +2).
    assert_eq!(
        email.text,
        "https://a.example/x?y=1&z=2 2026-03-29 03:30 CEST"
    );
    for bad in [
        "http://app.example.com/x",
        "javascript:alert(1)",
        "http://127.0.0.1:8080/x",
        "https://exa mple.com",
        "/relative",
    ] {
        assert_eq!(
            render(&checked, "tasks.assigned", Channel::Email, "en", &data(bad)),
            Err("invalid_url"),
            "{bad}"
        );
    }
    let local = project(true).configure().unwrap();
    assert!(
        render(
            &local,
            "tasks.assigned",
            Channel::Email,
            "en",
            &data("http://127.0.0.1:8080/x")
        )
        .is_ok()
    );
    assert_eq!(
        render(
            &local,
            "tasks.assigned",
            Channel::Email,
            "en",
            &data("http://app.example.com/x")
        ),
        Err("invalid_url")
    );
    let bad_date = object(json!({"title": "x", "link": "https://a.example", "at": "yesterday"}));
    assert_eq!(
        render(&checked, "tasks.assigned", Channel::Email, "en", &bad_date),
        Err("invalid_datetime")
    );
}

#[test]
fn sms_segments_and_limits_are_enforced() {
    let checked = Project::new(&project_topic("\"sms\"").replace(
        "[modules.notifications.topics",
        "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\nmax_segments = 2\n\n[modules.notifications.topics",
    ))
    .template("tasks.assigned/en/sms.txt", "{{ text }}")
    .template("tasks.assigned/template.toml", "[preview]\ntext = \"Hi\"\n")
    .configure()
    .unwrap();
    let sms = |text: &str| {
        render(
            &checked,
            "tasks.assigned",
            Channel::Sms,
            "en",
            &object(json!({"text": text})),
        )
    };
    let Ok(Rendered::Sms(gsm)) = sms(&"a".repeat(200)) else {
        panic!("renders")
    };
    assert_eq!((gsm.encoding, gsm.segments), (Encoding::Gsm7, 2));
    let Ok(Rendered::Sms(ucs)) = sms("Привет") else {
        panic!("renders")
    };
    assert_eq!((ucs.encoding, ucs.segments), (Encoding::Ucs2, 1));
    assert_eq!(sms(&"a".repeat(307)), Err("too_many_segments"));
}

#[test]
fn startup_checks_report_every_template_problem() {
    let issues = Project::new(&project_topic("\"email\", \"inbox\""))
        // A syntax error.
        .template("tasks.assigned/en/subject.txt", "{% if %}")
        // HTML without its text part.
        .template("tasks.assigned/en/body.html", "<p>x</p>")
        // The inbox never sees secret data.
        .template(
            "tasks.assigned/en/inbox.toml",
            "title = \"t\"\nbody = \"{{ secret.code }}\"\n",
        )
        // A locale outside the closed list and an unknown file.
        .template("tasks.assigned/de/subject.txt", "x")
        .template("tasks.assigned/en/body.md", "x")
        .template("Bad Name/en/body.txt", "x")
        .issues();
    let t = "modules.notifications.templates";
    for (key, code) in [
        (
            format!("{t}.\"tasks.assigned/en/subject.txt\""),
            "template_syntax",
        ),
        (
            format!("{t}.\"tasks.assigned/en/body.html\""),
            "template_text_missing",
        ),
        (
            format!("{t}.\"tasks.assigned/en/inbox.toml\""),
            "inbox_references_secret",
        ),
        (
            format!("{t}.\"tasks.assigned/de/subject.txt\""),
            "unknown_locale",
        ),
        (
            format!("{t}.\"tasks.assigned/en/body.md\""),
            "unknown_template_file",
        ),
        (format!("{t}.Bad Name"), "invalid_template_name"),
        (
            "modules.notifications.topics.\"tasks.assigned\".template".to_owned(),
            "template_missing",
        ),
    ] {
        assert!(has(&issues, &key, code), "{key} {code} not in {issues:?}");
    }
}

#[test]
fn a_project_file_replaces_the_embedded_default() {
    let checked = Project::new(MINIMAL)
        .template(
            "identity.magic_link/en/subject.txt",
            "Sign in to {{ app.name }} now",
        )
        .configure()
        .unwrap();
    let secret = object(json!({"link": "https://app.example.com/m/1"}));
    let context = RenderContext {
        topic: "identity.magic_link",
        category: Category::Security,
        notification_id: PREVIEW_NOTIFICATION_ID,
        data: &Map::new(),
        secret: &secret,
        recipient_name: None,
        timezone: None,
        unsubscribe: Some("https://x.example/u"),
    };
    let Ok(Rendered::Email(email)) =
        checked
            .registry
            .render("identity.magic_link", Channel::Email, "en", &context)
    else {
        panic!("renders");
    };
    assert_eq!(email.subject, "Sign in to Tasks now");
    assert!(email.text.contains("https://app.example.com/m/1"));
    // Security mail carries no unsubscribe link, even when one is passed.
    assert!(!email.text.contains("x.example/u"));
    assert!(!email.html.unwrap().contains("x.example/u"));
}

#[test]
fn inbox_renders_never_see_secret_data_even_through_a_partial() {
    let checked = Project::new(&project_topic("\"email\", \"inbox\""))
        .template("_partials/code.txt", "code {{ secret.code }}")
        .template("tasks.assigned/en/subject.txt", "Your code")
        .template("tasks.assigned/en/body.txt", "{% include \"_partials/code.txt\" %}")
        .template(
            "tasks.assigned/en/inbox.toml",
            "title = \"Code\"\nbody = \"{% if show %}{% include '_partials/code.txt' %}{% else %}hidden{% endif %}\"\n",
        )
        .template(
            "tasks.assigned/template.toml",
            "[preview]\nshow = false\n[preview.secret]\ncode = \"123456\"\n",
        )
        .configure()
        .unwrap();
    let secret = object(json!({"code": "123456"}));
    let data = object(json!({"show": true}));
    let context = RenderContext {
        topic: "tasks.assigned",
        category: Category::Transactional,
        notification_id: PREVIEW_NOTIFICATION_ID,
        data: &data,
        secret: &secret,
        recipient_name: None,
        timezone: None,
        unsubscribe: None,
    };
    // Email renders the secret through the partial.
    let Ok(Rendered::Email(email)) =
        checked
            .registry
            .render("tasks.assigned", Channel::Email, "en", &context)
    else {
        panic!("email renders");
    };
    assert_eq!(email.text, "code 123456");
    // The inbox refuses explicitly, with secret data present.
    let refused = checked
        .registry
        .render("tasks.assigned", Channel::Inbox, "en", &context)
        .unwrap_err();
    assert_eq!(refused.code, "inbox_references_secret");
    // Testing, printing or converting `secret` itself is refused too, and
    // never panics in the engine.
    for body in [
        "{{ secret|string }}",
        "{{ secret|safe }}",
        "{{ secret|lower }}",
        "{{ secret|truncate_chars(3) }}",
        "{{ secret ~ '' }}",
        "{{ secret }}",
        "{% if secret %}x{% endif %}y",
        "{% for k in secret %}{{ k }}{% endfor %}z",
    ] {
        let issues = Project::new(&project_topic("\"inbox\""))
            .template(
                "tasks.assigned/en/inbox.toml",
                "title = \"t\"\nbody = \"{% include '_partials/p.txt' %}\"\n",
            )
            .template("_partials/p.txt", body)
            .issues();
        assert!(
            has(
                &issues,
                "modules.notifications.templates.\"tasks.assigned\".preview.en.inbox",
                "inbox_references_secret"
            ),
            "{body}: {issues:?}"
        );
    }
}

#[test]
fn output_is_capped_while_rendering_not_after() {
    let started = std::time::Instant::now();
    let issues = Project::new(&project_topic("\"email\""))
        .template("tasks.assigned/en/subject.txt", "Big")
        .template(
            "tasks.assigned/en/body.txt",
            "{% for i in range(20) %}{{ 'x' * 50000000 }}{% endfor %}",
        )
        .issues();
    assert!(
        has(
            &issues,
            "modules.notifications.templates.\"tasks.assigned\".preview.en.email",
            "output_too_large"
        ),
        "{issues:?}"
    );
    // The first 50 MB piece hits the 128 KiB text cap; the other 19 never run.
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // The cap stops the render where it is hit: the undefined variable after
    // the oversized output is never reached.
    let issues = Project::new(&project_topic("\"email\""))
        .template("tasks.assigned/en/subject.txt", "Big")
        .template(
            "tasks.assigned/en/body.txt",
            "{{ 'x' * 200000 }}{{ never_defined }}",
        )
        .issues();
    assert!(
        has(
            &issues,
            "modules.notifications.templates.\"tasks.assigned\".preview.en.email",
            "output_too_large"
        ),
        "{issues:?}"
    );
}

#[test]
fn sms_refuses_every_control_character_but_line_feed() {
    let checked = Project::new(&project_topic("\"sms\"").replace(
        "[modules.notifications.topics",
        "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\n\n[modules.notifications.topics",
    ))
    .template("tasks.assigned/en/sms.txt", "{{ text }}")
    .template("tasks.assigned/template.toml", "[preview]\ntext = \"Hi\"\n")
    .configure()
    .unwrap();
    for bad in [
        "a\tb",
        "a\u{7}b",
        "a\rb",
        "a\u{1b}b",
        "a\u{85}b",
        "a\u{2028}b",
        "a\u{2029}b",
        "a\u{202a}b",
        "a\u{202e}b",
        "a\u{2066}b",
        "a\u{2069}b",
    ] {
        assert_eq!(
            render(
                &checked,
                "tasks.assigned",
                Channel::Sms,
                "en",
                &object(json!({"text": bad}))
            ),
            Err("control_character"),
            "{bad:?}"
        );
    }
    assert!(
        render(
            &checked,
            "tasks.assigned",
            Channel::Sms,
            "en",
            &object(json!({"text": "a\nb"}))
        )
        .is_ok()
    );
}
