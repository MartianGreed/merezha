//! Scalar field values: one check per value, returning the stored form or
//! the detail code of the first rule it breaks.
use crate::model::{
    DecimalRules, EnumRules, FieldKind, FloatRules, IntegerRules, MAX_SAFE_INTEGER, SlugRules,
    StringRules, TextRange, TextRules,
};
use rust_decimal::Decimal;
use serde_json::Value;

/// Check a scalar value against its field type; `Ok` holds the stored
/// form (decimals at their scale, datetimes in UTC). Non-scalar kinds are
/// a `type_mismatch`.
pub(crate) fn check(kind: &FieldKind, value: &Value) -> Result<Value, &'static str> {
    match kind {
        FieldKind::String(rules) => string(rules, value),
        FieldKind::Text(rules) => text(rules, value),
        FieldKind::Integer(rules) => integer(rules, value),
        FieldKind::Float(rules) => float(rules, value),
        FieldKind::Decimal(rules) => decimal_value(rules, value),
        FieldKind::Boolean => match value {
            Value::Bool(_) => Ok(value.clone()),
            _ => Err("type_mismatch"),
        },
        FieldKind::Datetime(range) => {
            let normalized = datetime(as_str(value)?)?;
            in_range(range, normalized)
        }
        FieldKind::Date(range) => {
            let normalized = date(as_str(value)?)?;
            in_range(range, normalized)
        }
        FieldKind::Enum(rules) => enumeration(rules, value),
        FieldKind::Slug(rules) => slug(rules, value),
        _ => Err("type_mismatch"),
    }
}

fn as_str(value: &Value) -> Result<&str, &'static str> {
    value.as_str().ok_or("type_mismatch")
}

/// Unicode scalar values in `text`.
pub(crate) fn length(text: &str) -> usize {
    text.chars().count()
}

fn lengths(text: &str, min: Option<u32>, max: u32) -> Result<(), &'static str> {
    let count = length(text);
    if min.is_some_and(|min| count < min as usize) {
        return Err("min_length");
    }
    if count > max as usize {
        return Err("max_length");
    }
    Ok(())
}

fn string(rules: &StringRules, value: &Value) -> Result<Value, &'static str> {
    let text = as_str(value)?;
    if text.contains(['\0', '\r', '\n']) {
        return Err("invalid_character");
    }
    lengths(text, rules.min_length, rules.max_length)?;
    if rules.pattern.as_ref().is_some_and(|p| !p.is_match(text)) {
        return Err("pattern");
    }
    Ok(value.clone())
}

fn text(rules: &TextRules, value: &Value) -> Result<Value, &'static str> {
    let text = as_str(value)?;
    if text.contains('\0') {
        return Err("invalid_character");
    }
    lengths(text, rules.min_length, rules.max_length)?;
    Ok(value.clone())
}

fn integer(rules: &IntegerRules, value: &Value) -> Result<Value, &'static str> {
    let Value::Number(number) = value else {
        return Err("type_mismatch");
    };
    let n = if let Some(n) = number.as_i64() {
        n
    } else if number.is_u64() {
        return Err("max");
    } else {
        // A binary64 value. serde_json parses JSON `-0` as -0.0 and cannot
        // tell it from `-0.0` or `-0e0`, so all three are accepted as the
        // integer 0. Integral values beyond the safe range are out of range,
        // and anything else, `1.0` included, is not an integer.
        let f = number.as_f64().unwrap_or(f64::NAN);
        if f == 0.0 && f.is_sign_negative() {
            0
        } else if f.fract() == 0.0 && f.abs() > MAX_SAFE_INTEGER as f64 {
            return Err(if f > 0.0 { "max" } else { "min" });
        } else {
            return Err("type_mismatch");
        }
    };
    if n < -MAX_SAFE_INTEGER || rules.min.is_some_and(|min| n < min) {
        return Err("min");
    }
    if n > MAX_SAFE_INTEGER || rules.max.is_some_and(|max| n > max) {
        return Err("max");
    }
    Ok(Value::from(n))
}

fn float(rules: &FloatRules, value: &Value) -> Result<Value, &'static str> {
    let Value::Number(number) = value else {
        return Err("type_mismatch");
    };
    let n = number.as_f64().ok_or("type_mismatch")?;
    // The application JSON policy keeps integral values in the safe range;
    // beyond it a number is out of range rather than of the wrong type.
    if n.fract() == 0.0 && n.abs() > MAX_SAFE_INTEGER as f64 {
        return Err(if n > 0.0 { "max" } else { "min" });
    }
    if rules.min.is_some_and(|min| n < min) {
        return Err("min");
    }
    if rules.max.is_some_and(|max| n > max) {
        return Err("max");
    }
    Ok(value.clone())
}

/// `^-?(0|[1-9][0-9]*)(\.[0-9]+)?$` normalized to exactly `scale`
/// fractional digits. More fractional digits than `scale` is `scale`, more
/// integer digits than `precision - scale` is `precision`; nothing rounds.
pub(crate) fn decimal(text: &str, precision: u32, scale: u32) -> Result<String, &'static str> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (int, frac) = match unsigned.split_once('.') {
        Some((int, frac)) => (int, Some(frac)),
        None => (unsigned, None),
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(int) || (int.len() > 1 && int.starts_with('0')) || frac.is_some_and(|f| !digits(f)) {
        return Err("type_mismatch");
    }
    let frac = frac.unwrap_or("");
    if frac.len() > scale as usize {
        return Err("scale");
    }
    let int_digits = if int == "0" { 0 } else { int.len() };
    if int_digits > precision.saturating_sub(scale) as usize {
        return Err("precision");
    }
    let zero = int == "0" && frac.bytes().all(|b| b == b'0');
    let mut out = String::with_capacity(int.len() + scale as usize + 2);
    if negative && !zero {
        out.push('-');
    }
    out.push_str(int);
    if scale > 0 {
        out.push('.');
        out.push_str(frac);
        out.extend(std::iter::repeat_n('0', scale as usize - frac.len()));
    }
    Ok(out)
}

/// A normalized decimal as a number, for range checks.
pub(crate) fn decimal_number(normalized: &str) -> Option<Decimal> {
    Decimal::from_str_exact(normalized).ok()
}

fn decimal_value(rules: &DecimalRules, value: &Value) -> Result<Value, &'static str> {
    let normalized = decimal(as_str(value)?, rules.precision, rules.scale)?;
    let number = decimal_number(&normalized).ok_or("precision")?;
    let bound = |b: &Option<String>| b.as_deref().and_then(decimal_number);
    if bound(&rules.min).is_some_and(|min| number < min) {
        return Err("min");
    }
    if bound(&rules.max).is_some_and(|max| number > max) {
        return Err("max");
    }
    Ok(Value::String(normalized))
}

fn number(bytes: &[u8]) -> Option<i32> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    bytes.iter().try_fold(0i32, |acc, b| {
        acc.checked_mul(10)?.checked_add(i32::from(b - b'0'))
    })
}

/// RFC 3339 with an offset (`T`/`Z` in either case), at most 3 fractional
/// digits, stored as UTC `YYYY-MM-DDTHH:MM:SS.sssZ`.
pub(crate) fn datetime(text: &str) -> Result<String, &'static str> {
    const MISMATCH: &str = "type_mismatch";
    let b = text.as_bytes();
    if b.len() < 20 || b.len() > 64 {
        return Err(MISMATCH);
    }
    let separators = b[4] == b'-'
        && b[7] == b'-'
        && (b[10] == b'T' || b[10] == b't')
        && b[13] == b':'
        && b[16] == b':';
    if !separators {
        return Err(MISMATCH);
    }
    let part = |range: std::ops::Range<usize>| number(&b[range]).ok_or(MISMATCH);
    let (year, month, day) = (part(0..4)?, part(5..7)?, part(8..10)?);
    let (hour, minute, second) = (part(11..13)?, part(14..16)?, part(17..19)?);
    let mut at = 19;
    let mut nanos = 0i32;
    if b[at] == b'.' {
        let start = at + 1;
        let mut end = start;
        while end < b.len() && b[end].is_ascii_digit() {
            end += 1;
        }
        let count = end - start;
        if count == 0 {
            return Err(MISMATCH);
        }
        if count > 3 {
            return Err("datetime_precision");
        }
        let millis = number(&b[start..end]).ok_or(MISMATCH)? * 10i32.pow(3 - count as u32);
        nanos = millis * 1_000_000;
        at = end;
    }
    let offset_seconds = match &b[at..] {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), h1, h2, b':', m1, m2] => {
            let hours = number(&[*h1, *h2]).ok_or(MISMATCH)?;
            let minutes = number(&[*m1, *m2]).ok_or(MISMATCH)?;
            if hours > 23 || minutes > 59 {
                return Err(MISMATCH);
            }
            let seconds = hours * 3600 + minutes * 60;
            if *sign == b'-' { -seconds } else { seconds }
        }
        _ => return Err(MISMATCH),
    };
    let narrow = |n: i32| i8::try_from(n).map_err(|_| MISMATCH);
    let civil = jiff::civil::DateTime::new(
        i16::try_from(year).map_err(|_| MISMATCH)?,
        narrow(month)?,
        narrow(day)?,
        narrow(hour)?,
        narrow(minute)?,
        narrow(second)?,
        nanos,
    )
    .map_err(|_| MISMATCH)?;
    let offset = jiff::tz::Offset::from_seconds(offset_seconds).map_err(|_| MISMATCH)?;
    let instant = offset.to_timestamp(civil).map_err(|_| MISMATCH)?;
    let utc = jiff::tz::Offset::UTC.to_datetime(instant);
    if !(0..=9999).contains(&utc.year()) {
        return Err(MISMATCH);
    }
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        utc.year(),
        utc.month(),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second(),
        utc.subsec_nanosecond() / 1_000_000
    ))
}

/// `YYYY-MM-DD`, a calendar date with no zone.
pub(crate) fn date(text: &str) -> Result<String, &'static str> {
    let b = text.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err("type_mismatch");
    }
    let part = |range: std::ops::Range<usize>| number(&b[range]).ok_or("type_mismatch");
    let (year, month, day) = (part(0..4)?, part(5..7)?, part(8..10)?);
    let narrow = |n: i32| i8::try_from(n).map_err(|_| "type_mismatch");
    jiff::civil::Date::new(
        i16::try_from(year).map_err(|_| "type_mismatch")?,
        narrow(month)?,
        narrow(day)?,
    )
    .map_err(|_| "type_mismatch")?;
    Ok(text.to_owned())
}

/// Stored dates and datetimes are fixed width, so text order is time order.
fn in_range(range: &TextRange, normalized: String) -> Result<Value, &'static str> {
    if range
        .min
        .as_deref()
        .is_some_and(|min| normalized.as_str() < min)
    {
        return Err("min");
    }
    if range
        .max
        .as_deref()
        .is_some_and(|max| normalized.as_str() > max)
    {
        return Err("max");
    }
    Ok(Value::String(normalized))
}

fn enumeration(rules: &EnumRules, value: &Value) -> Result<Value, &'static str> {
    let text = as_str(value)?;
    if rules.values.iter().any(|v| v == text) {
        Ok(value.clone())
    } else {
        Err("not_in_enum")
    }
}

/// `^[a-z0-9]+(?:-[a-z0-9]+)*$`.
pub(crate) fn is_slug(text: &str) -> bool {
    !text.is_empty()
        && !text.starts_with('-')
        && !text.ends_with('-')
        && !text.contains("--")
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

fn slug(rules: &SlugRules, value: &Value) -> Result<Value, &'static str> {
    let text = as_str(value)?;
    if text.contains('\0') {
        return Err("invalid_character");
    }
    if text.len() > rules.max_length as usize {
        return Err("max_length");
    }
    if !is_slug(text) {
        return Err("pattern");
    }
    Ok(value.clone())
}
