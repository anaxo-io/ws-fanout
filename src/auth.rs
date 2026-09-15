//! Who a connection is, and what it may subscribe to.
//!
//! The server does not know what a token is. It hands the string from the client's `auth`
//! message to a [`TokenValidator`] and gets back [`Claims`] or a refusal. It then asks an
//! [`Authorizer`] whether those claims may subscribe to each channel. Both are traits so
//! the crate carries no opinion about tiers, plans, or identity providers.
//!
//! With the `jwt` feature, [`HmacJwt`] validates HS256 tokens.

use std::collections::BTreeMap;

use serde_json::Value;

/// What a validated token says about the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The principal — typically the token's `sub`.
    pub subject: String,
    /// Everything else the token carried, for an [`Authorizer`] to inspect.
    pub attributes: BTreeMap<String, Value>,
}

impl Claims {
    /// Claims with only a subject.
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            attributes: BTreeMap::new(),
        }
    }

    /// A string attribute, if present and a string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).and_then(Value::as_str)
    }
}

/// Turns the client's token into [`Claims`], or refuses it.
///
/// The `Err` string is sent to the client verbatim in an `error` message before the
/// connection is closed, so it should say *that* the token was refused and not *why* in
/// any detail an attacker would find useful.
pub trait TokenValidator: Send + Sync + 'static {
    /// Validate `token`.
    fn validate(&self, token: &str) -> Result<Claims, String>;
}

/// Decides, per channel, whether a connection may subscribe.
pub trait Authorizer: Send + Sync + 'static {
    /// Whether `claims` may subscribe to `channel`.
    fn may_subscribe(&self, claims: &Claims, channel: &str) -> bool;
}

/// Accepts every subscription. The default.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl Authorizer for AllowAll {
    fn may_subscribe(&self, _: &Claims, _: &str) -> bool {
        true
    }
}

impl<F> Authorizer for F
where
    F: Fn(&Claims, &str) -> bool + Send + Sync + 'static,
{
    fn may_subscribe(&self, claims: &Claims, channel: &str) -> bool {
        self(claims, channel)
    }
}

/// Accepts exactly one fixed token and maps it to a subject. For tests and demos.
#[derive(Debug, Clone)]
pub struct StaticToken {
    token: String,
    subject: String,
}

impl StaticToken {
    /// Accept `token` as `subject`.
    pub fn new(token: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            subject: subject.into(),
        }
    }
}

impl TokenValidator for StaticToken {
    fn validate(&self, token: &str) -> Result<Claims, String> {
        if token == self.token {
            Ok(Claims::new(self.subject.clone()))
        } else {
            Err("invalid token".to_string())
        }
    }
}

/// HS256 JSON Web Tokens with a shared secret.
///
/// Requires `exp`; the `sub` claim becomes [`Claims::subject`] and every other claim lands
/// in [`Claims::attributes`], so an [`Authorizer`] can read a `tier` or `scope` without the
/// server knowing those words.
#[cfg(feature = "jwt")]
#[derive(Clone)]
pub struct HmacJwt {
    key: jsonwebtoken::DecodingKey,
    validation: jsonwebtoken::Validation,
}

#[cfg(feature = "jwt")]
impl std::fmt::Debug for HmacJwt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmacJwt").finish_non_exhaustive()
    }
}

#[cfg(feature = "jwt")]
impl HmacJwt {
    /// Validate tokens signed with `secret`.
    pub fn new(secret: &[u8]) -> Self {
        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_required_spec_claims(&["exp", "sub"]);
        Self {
            key: jsonwebtoken::DecodingKey::from_secret(secret),
            validation,
        }
    }
}

#[cfg(feature = "jwt")]
impl TokenValidator for HmacJwt {
    fn validate(&self, token: &str) -> Result<Claims, String> {
        let data = jsonwebtoken::decode::<serde_json::Map<String, Value>>(
            token,
            &self.key,
            &self.validation,
        )
        .map_err(|_| "invalid token".to_string())?;

        let mut attributes: BTreeMap<String, Value> = data.claims.into_iter().collect();
        let subject = match attributes.remove("sub") {
            Some(Value::String(s)) => s,
            _ => return Err("invalid token".to_string()),
        };
        Ok(Claims {
            subject,
            attributes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_token_accepts_only_its_token() {
        let v = StaticToken::new("secret", "alice");
        assert_eq!(v.validate("secret").unwrap().subject, "alice");
        assert!(v.validate("other").is_err());
    }

    #[test]
    fn closures_are_authorizers() {
        let only_public = |c: &Claims, ch: &str| ch.starts_with("public:") || c.subject == "admin";
        assert!(only_public.may_subscribe(&Claims::new("bob"), "public:x"));
        assert!(!only_public.may_subscribe(&Claims::new("bob"), "private:x"));
        assert!(only_public.may_subscribe(&Claims::new("admin"), "private:x"));
    }

    #[cfg(feature = "jwt")]
    #[test]
    fn hmac_jwt_round_trip() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        let secret = b"test-secret";
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 60;
        let claims = serde_json::json!({"sub": "alice", "tier": "pro", "exp": exp});
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap();

        let v = HmacJwt::new(secret);
        let c = v.validate(&token).unwrap();
        assert_eq!(c.subject, "alice");
        assert_eq!(c.get_str("tier"), Some("pro"));
        assert!(v.validate("garbage").is_err());
        assert!(HmacJwt::new(b"wrong").validate(&token).is_err());
    }
}
