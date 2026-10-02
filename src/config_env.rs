//! Expand inherited environment variables only after YAML has been parsed.

use std::env::VarError;

use anyhow::{bail, Context, Result};
use serde::de::DeserializeOwned;
use serde_yaml::Value;

pub(crate) fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    mut lookup: impl FnMut(&str) -> Result<String, VarError>,
) -> Result<T> {
    let mut value: Value = serde_yaml::from_slice(bytes).map_err(|error| {
        // YAML errors can quote plaintext credentials from the input.
        match error.location() {
            Some(location) => anyhow::anyhow!(
                "invalid YAML at line {}, column {}",
                location.line(),
                location.column()
            ),
            None => anyhow::anyhow!("invalid YAML config"),
        }
    })?;
    visit_strings(&mut value, "$", &mut |text, path| {
        *text = expand(text, &mut lookup).with_context(|| format!("at config {path}"))?;
        Ok(())
    })?;
    match serde_yaml::from_value(value.clone()) {
        Ok(config) => Ok(config),
        Err(_) => {
            // Reproduce schema errors with redacted values rather than exposing
            // a resolved secret in serde's "invalid value" diagnostics.
            visit_strings(&mut value, "$", &mut |text, _| {
                *text = "[REDACTED]".into();
                Ok(())
            })?;
            match serde_yaml::from_value::<T>(value) {
                Err(error) => bail!("invalid config schema: {error}"),
                Ok(_) => bail!("invalid config schema (values omitted)"),
            }
        }
    }
}

fn visit_strings(
    value: &mut Value,
    path: &str,
    visit: &mut impl FnMut(&mut String, &str) -> Result<()>,
) -> Result<()> {
    match value {
        Value::String(text) => visit(text, path)?,
        Value::Sequence(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                visit_strings(value, &format!("{path}[{index}]"), visit)?;
            }
        }
        Value::Mapping(values) => {
            for (index, (key, value)) in values.iter_mut().enumerate() {
                let path = match key.as_str() {
                    Some(key) => format!("{path}[{key:?}]"),
                    None => format!("{path}[entry {index}]"),
                };
                visit_strings(value, &path, visit)?;
            }
        }
        Value::Tagged(value) => visit_strings(&mut value.value, path, visit)?,
        _ => {}
    }
    Ok(())
}

fn expand(
    input: &str,
    lookup: &mut impl FnMut(&str) -> Result<String, VarError>,
) -> Result<String> {
    let mut output = String::with_capacity(input.len());
    let mut remaining = input;
    while let Some(index) = remaining.find('$') {
        output.push_str(&remaining[..index]);
        remaining = &remaining[index + 1..];
        if let Some(rest) = remaining.strip_prefix('$') {
            output.push('$');
            remaining = rest;
        } else if let Some(rest) = remaining.strip_prefix('{') {
            let Some(end) = rest.find('}') else {
                bail!("unterminated environment variable expression");
            };
            let name = &rest[..end];
            if name.is_empty()
                || !name.bytes().enumerate().all(|(i, byte)| {
                    byte == b'_' || byte.is_ascii_alphabetic() || (i > 0 && byte.is_ascii_digit())
                })
            {
                bail!("invalid environment variable expression; expected ${{NAME}}");
            }
            let value = lookup(name).map_err(|error| match error {
                VarError::NotPresent => anyhow::anyhow!("environment variable {name} is not set"),
                VarError::NotUnicode(_) => {
                    anyhow::anyhow!("environment variable {name} is not valid Unicode")
                }
            })?;
            output.push_str(&value);
            remaining = &rest[end + 1..];
        } else {
            output.push('$');
        }
    }
    output.push_str(remaining);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_is_single_pass_and_preserves_literal_dollars() {
        let mut lookup = |name: &str| Ok(format!("<{name}>${{UNCHANGED}}"));
        assert_eq!(
            expand("å-${USER}-${_A2}/$$/$${ESCAPED}/$BARE/$", &mut lookup).unwrap(),
            "å-<USER>${UNCHANGED}-<_A2>${UNCHANGED}/$/${ESCAPED}/$BARE/$"
        );
    }

    #[test]
    fn invalid_expressions_fail_without_lookup_or_echoing_input() {
        for input in [
            "${",
            "${}",
            "${2BAD}",
            "${A:-secret}",
            "${A B}",
            "${Ä}",
            "${A${B}}",
        ] {
            let error = expand(input, &mut |_| panic!("unexpected lookup")).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn missing_and_non_unicode_errors_are_safe() {
        let error = decode::<Value>(b"nested: [\"${MISSING}\"]", |_| Err(VarError::NotPresent))
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("MISSING"));
        assert!(message.contains("$[\"nested\"][0]"));
        let error = expand("${SECRET}", &mut |_| {
            Err(VarError::NotUnicode("do-not-print".into()))
        })
        .unwrap_err();
        assert!(error.to_string().contains("SECRET"));
        assert!(!format!("{error:#}").contains("do-not-print"));
        assert_eq!(expand("${EMPTY}", &mut |_| Ok(String::new())).unwrap(), "");
    }

    #[test]
    fn yaml_values_cannot_inject_structure_and_keys_are_unchanged() {
        let secret = "\"quoted\": true\n- [null, 123]\n${OTHER}";
        let value: Value =
            decode(b"'${KEY}': [\"${VALUE}\", 42, true]", |_| Ok(secret.into())).unwrap();
        let items = value["${KEY}"].as_sequence().unwrap();
        assert_eq!(items[0].as_str(), Some(secret));
        assert_eq!(items[1].as_u64(), Some(42));
        assert_eq!(items[2].as_bool(), Some(true));
    }

    #[test]
    fn typed_errors_do_not_leak_values_or_coerce_scalars() {
        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct Typed {
            count: u32,
            enabled: bool,
        }
        for (yaml, replacement) in [
            ("count: '${VALUE}'\nenabled: true", "123"),
            ("count: 1\nenabled: '${VALUE}'", "true"),
            ("count: '${VALUE}'\nenabled: true", "private-secret"),
        ] {
            let error = decode::<Typed>(yaml.as_bytes(), |_| Ok(replacement.into())).unwrap_err();
            assert!(!format!("{error:#}").contains(replacement));
        }
        let error = decode::<Value>(b"secret: [private-secret", |_| unreachable!()).unwrap_err();
        assert!(!format!("{error:#}").contains("private-secret"));
    }
}
