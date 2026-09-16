use std::str::FromStr;

/// Reads a mandatory variable. Missing configuration is a deployment bug,
/// so we fail loudly at startup instead of limping along with a guess.
pub fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var {name}"))
}

pub fn or_default(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

pub fn parse_or<T: FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Identity of this process as seen on the chaos panel scheme.
pub fn service_id() -> String {
    required("SERVICE_ID")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn or_default_falls_back_when_unset() {
        assert_eq!(or_default("COMMON_TEST_UNSET_VAR", "x"), "x");
    }

    #[test]
    fn parse_or_ignores_garbage() {
        // SAFETY: test-only, no other threads read this variable.
        unsafe { std::env::set_var("COMMON_TEST_GARBAGE", "not-a-number") };
        assert_eq!(parse_or("COMMON_TEST_GARBAGE", 7u16), 7);
    }
}
