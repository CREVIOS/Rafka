#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;

pub const ERROR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub const ERROR_ILLEGAL_SASL_STATE: i16 = 34;
pub const ERROR_SASL_AUTHENTICATION_FAILED: i16 = 58;

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-256" => Some(Self::ScramSha256),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslConfigError {
    EmptyUsername,
    InvalidIterationCount(u32),
}

#[derive(Debug, Clone)]
struct ScramUser {
    salt: Vec<u8>,
    iterations: u32,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct SaslConfig {
    mechanisms: BTreeSet<SaslMechanism>,
    plain_users: BTreeMap<String, String>,
    scram_sha256_users: BTreeMap<String, ScramUser>,
    scram_sha512_users: BTreeMap<String, ScramUser>,
}

impl SaslConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_plain_user(
        &mut self,
        username: &str,
        password: &str,
    ) -> Result<(), SaslConfigError> {
        if username.is_empty() {
            return Err(SaslConfigError::EmptyUsername);
        }
        self.mechanisms.insert(SaslMechanism::Plain);
        self.plain_users
            .insert(username.to_string(), password.to_string());
        Ok(())
    }

    pub fn add_scram_sha256_user(
        &mut self,
        username: &str,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<(), SaslConfigError> {
        if username.is_empty() {
            return Err(SaslConfigError::EmptyUsername);
        }
        if iterations == 0 {
            return Err(SaslConfigError::InvalidIterationCount(iterations));
        }
        let (stored_key, server_key) = derive_scram_secret(
            SaslMechanism::ScramSha256,
            password.as_bytes(),
            &salt,
            iterations,
        );
        self.mechanisms.insert(SaslMechanism::ScramSha256);
        self.scram_sha256_users.insert(
            username.to_string(),
            ScramUser {
                salt,
                iterations,
                stored_key,
                server_key,
            },
        );
        Ok(())
    }

    pub fn add_scram_sha512_user(
        &mut self,
        username: &str,
        password: &str,
        salt: Vec<u8>,
        iterations: u32,
    ) -> Result<(), SaslConfigError> {
        if username.is_empty() {
            return Err(SaslConfigError::EmptyUsername);
        }
        if iterations == 0 {
            return Err(SaslConfigError::InvalidIterationCount(iterations));
        }
        let (stored_key, server_key) = derive_scram_secret(
            SaslMechanism::ScramSha512,
            password.as_bytes(),
            &salt,
            iterations,
        );
        self.mechanisms.insert(SaslMechanism::ScramSha512);
        self.scram_sha512_users.insert(
            username.to_string(),
            ScramUser {
                salt,
                iterations,
                stored_key,
                server_key,
            },
        );
        Ok(())
    }

    pub fn enabled_mechanisms(&self) -> Vec<String> {
        self.mechanisms
            .iter()
            .map(|mechanism| mechanism.as_str().to_string())
            .collect()
    }

    fn supports_mechanism(&self, mechanism: SaslMechanism) -> bool {
        self.mechanisms.contains(&mechanism)
    }

    fn plain_password(&self, username: &str) -> Option<&str> {
        self.plain_users.get(username).map(String::as_str)
    }

    fn scram_user(&self, mechanism: SaslMechanism, username: &str) -> Option<&ScramUser> {
        match mechanism {
            SaslMechanism::ScramSha256 => self.scram_sha256_users.get(username),
            SaslMechanism::ScramSha512 => self.scram_sha512_users.get(username),
            SaslMechanism::Plain => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SaslHandshakeOutcome {
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SaslAuthenticateOutcome {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
    pub complete: bool,
    pub principal: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SaslConnectionState {
    AwaitHandshake,
    AwaitAuthenticate {
        mechanism: SaslMechanism,
    },
    AwaitScramFinal {
        mechanism: SaslMechanism,
        username: String,
        client_first_bare: String,
        server_first: String,
        combined_nonce: String,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    },
    Authenticated {
        mechanism: SaslMechanism,
        principal: String,
    },
}

impl SaslConnectionState {
    pub fn new() -> Self {
        Self::AwaitHandshake
    }

    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    pub fn mechanism_name(&self) -> Option<&'static str> {
        match self {
            Self::AwaitAuthenticate { mechanism }
            | Self::AwaitScramFinal { mechanism, .. }
            | Self::Authenticated { mechanism, .. } => Some(mechanism.as_str()),
            Self::AwaitHandshake => None,
        }
    }

    pub fn principal(&self) -> Option<&str> {
        match self {
            Self::Authenticated { principal, .. } => Some(principal.as_str()),
            Self::AwaitHandshake
            | Self::AwaitAuthenticate { .. }
            | Self::AwaitScramFinal { .. } => None,
        }
    }
}

impl Default for SaslConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn begin_sasl_handshake(
    config: &SaslConfig,
    mechanism: &str,
    state: &mut SaslConnectionState,
) -> SaslHandshakeOutcome {
    let mechanisms = config.enabled_mechanisms();
    let Some(parsed) = SaslMechanism::parse(mechanism) else {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslHandshakeOutcome {
            error_code: ERROR_UNSUPPORTED_SASL_MECHANISM,
            mechanisms,
        };
    };
    if !config.supports_mechanism(parsed) {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslHandshakeOutcome {
            error_code: ERROR_UNSUPPORTED_SASL_MECHANISM,
            mechanisms,
        };
    }

    if matches!(state, SaslConnectionState::Authenticated { .. }) {
        return SaslHandshakeOutcome {
            error_code: ERROR_ILLEGAL_SASL_STATE,
            mechanisms,
        };
    }

    *state = SaslConnectionState::AwaitAuthenticate { mechanism: parsed };
    SaslHandshakeOutcome {
        error_code: 0,
        mechanisms,
    }
}

pub fn authenticate_sasl(
    config: &SaslConfig,
    auth_bytes: &[u8],
    state: &mut SaslConnectionState,
) -> SaslAuthenticateOutcome {
    let current = state.clone();
    match current {
        SaslConnectionState::AwaitAuthenticate { mechanism } => match mechanism {
            SaslMechanism::Plain => authenticate_plain(config, auth_bytes, state),
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                authenticate_scram_first(config, mechanism, auth_bytes, state)
            }
        },
        SaslConnectionState::AwaitScramFinal {
            mechanism,
            username,
            client_first_bare,
            server_first,
            combined_nonce,
            stored_key,
            server_key,
        } => authenticate_scram_final(
            mechanism,
            &username,
            &client_first_bare,
            &server_first,
            &combined_nonce,
            &stored_key,
            &server_key,
            auth_bytes,
            state,
        ),
        SaslConnectionState::Authenticated { .. } => SaslAuthenticateOutcome {
            error_code: ERROR_ILLEGAL_SASL_STATE,
            error_message: Some("SASL authentication already completed".to_string()),
            auth_bytes: Vec::new(),
            complete: true,
            principal: None,
        },
        SaslConnectionState::AwaitHandshake => SaslAuthenticateOutcome {
            error_code: ERROR_ILLEGAL_SASL_STATE,
            error_message: Some("SASL handshake must complete before authenticate".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        },
    }
}

fn authenticate_plain(
    config: &SaslConfig,
    auth_bytes: &[u8],
    state: &mut SaslConnectionState,
) -> SaslAuthenticateOutcome {
    let decoded = parse_plain_payload(auth_bytes);
    let (username, password) = match decoded {
        Ok(value) => value,
        Err(message) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some(message),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };

    match config.plain_password(&username) {
        Some(expected) if expected == password => {
            *state = SaslConnectionState::Authenticated {
                mechanism: SaslMechanism::Plain,
                principal: username.clone(),
            };
            SaslAuthenticateOutcome {
                error_code: 0,
                error_message: None,
                auth_bytes: Vec::new(),
                complete: true,
                principal: Some(username),
            }
        }
        _ => {
            *state = SaslConnectionState::AwaitHandshake;
            SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid username or password".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            }
        }
    }
}

fn authenticate_scram_first(
    config: &SaslConfig,
    mechanism: SaslMechanism,
    auth_bytes: &[u8],
    state: &mut SaslConnectionState,
) -> SaslAuthenticateOutcome {
    let client_first = match std::str::from_utf8(auth_bytes) {
        Ok(value) => value,
        Err(_) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid SCRAM client first message encoding".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };

    let (_, client_first_bare) = match client_first.split_once(",,") {
        Some(parts) => parts,
        None => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid SCRAM client first message".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };
    let attrs = match parse_scram_attributes(client_first_bare) {
        Ok(value) => value,
        Err(message) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some(message),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };
    let username = attrs
        .get(&'n')
        .map(|value| decode_scram_username(value))
        .unwrap_or_default();
    let client_nonce = attrs.get(&'r').cloned().unwrap_or_default();
    if username.is_empty() || client_nonce.is_empty() {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("SCRAM username and nonce are required".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    }
    let Some(user) = config.scram_user(mechanism, &username) else {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("Unknown SCRAM user".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    };

    let mut nonce_suffix = [0_u8; 18];
    OsRng.fill_bytes(&mut nonce_suffix);
    let combined_nonce = format!("{client_nonce}{}", STANDARD_NO_PAD.encode(nonce_suffix));
    let server_first = format!(
        "r={combined_nonce},s={},i={}",
        STANDARD_NO_PAD.encode(&user.salt),
        user.iterations
    );

    *state = SaslConnectionState::AwaitScramFinal {
        mechanism,
        username,
        client_first_bare: client_first_bare.to_string(),
        server_first: server_first.clone(),
        combined_nonce,
        stored_key: user.stored_key.clone(),
        server_key: user.server_key.clone(),
    };

    SaslAuthenticateOutcome {
        error_code: 0,
        error_message: None,
        auth_bytes: server_first.into_bytes(),
        complete: false,
        principal: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn authenticate_scram_final(
    mechanism: SaslMechanism,
    username: &str,
    client_first_bare: &str,
    server_first: &str,
    combined_nonce: &str,
    stored_key: &[u8],
    server_key: &[u8],
    auth_bytes: &[u8],
    state: &mut SaslConnectionState,
) -> SaslAuthenticateOutcome {
    let client_final = match std::str::from_utf8(auth_bytes) {
        Ok(value) => value,
        Err(_) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid SCRAM client final message encoding".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };
    let (without_proof, proof_b64) = match client_final.rsplit_once(",p=") {
        Some(parts) => parts,
        None => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("SCRAM proof is missing".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };
    let attrs = match parse_scram_attributes(without_proof) {
        Ok(value) => value,
        Err(message) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some(message),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };
    if attrs.get(&'r').map(String::as_str) != Some(combined_nonce) {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("SCRAM nonce mismatch".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    }
    if attrs.get(&'c').map(String::as_str) != Some("biws") {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("Unsupported SCRAM channel binding".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    }
    let proof = match STANDARD_NO_PAD.decode(proof_b64.as_bytes()) {
        Ok(value) => value,
        Err(_) => {
            *state = SaslConnectionState::AwaitHandshake;
            return SaslAuthenticateOutcome {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid SCRAM proof encoding".to_string()),
                auth_bytes: Vec::new(),
                complete: false,
                principal: None,
            };
        }
    };

    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_signature = compute_hmac(mechanism, stored_key, auth_message.as_bytes());
    if proof.len() != client_signature.len() {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("Invalid SCRAM proof length".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    }

    let mut client_key = vec![0_u8; proof.len()];
    for (index, proof_byte) in proof.iter().copied().enumerate() {
        client_key[index] = proof_byte ^ client_signature[index];
    }
    let derived_stored_key = compute_hash(mechanism, &client_key);
    if stored_key.len() != derived_stored_key.len()
        || stored_key.ct_eq(&derived_stored_key).unwrap_u8() != 1
    {
        *state = SaslConnectionState::AwaitHandshake;
        return SaslAuthenticateOutcome {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("SCRAM proof verification failed".to_string()),
            auth_bytes: Vec::new(),
            complete: false,
            principal: None,
        };
    }

    let server_signature = compute_hmac(mechanism, server_key, auth_message.as_bytes());
    let server_final = format!("v={}", STANDARD_NO_PAD.encode(server_signature));
    *state = SaslConnectionState::Authenticated {
        mechanism,
        principal: username.to_string(),
    };
    SaslAuthenticateOutcome {
        error_code: 0,
        error_message: None,
        auth_bytes: server_final.into_bytes(),
        complete: true,
        principal: Some(username.to_string()),
    }
}

fn parse_plain_payload(input: &[u8]) -> Result<(String, String), String> {
    let mut parts = input.split(|byte| *byte == 0_u8);
    let _authzid = parts.next().ok_or("Invalid PLAIN payload")?;
    let username = parts.next().ok_or("Invalid PLAIN payload")?;
    let password = parts.next().ok_or("Invalid PLAIN payload")?;
    if parts.next().is_some() {
        return Err("Invalid PLAIN payload".to_string());
    }
    let username = std::str::from_utf8(username)
        .map_err(|_| "Invalid username encoding".to_string())?
        .to_string();
    let password = std::str::from_utf8(password)
        .map_err(|_| "Invalid password encoding".to_string())?
        .to_string();
    if username.is_empty() {
        return Err("Empty username is not allowed".to_string());
    }
    Ok((username, password))
}

fn parse_scram_attributes(input: &str) -> Result<BTreeMap<char, String>, String> {
    let mut attrs = BTreeMap::new();
    for pair in input.split(',') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err("Malformed SCRAM attribute".to_string());
        };
        let mut chars = key.chars();
        let Some(key_char) = chars.next() else {
            return Err("Malformed SCRAM attribute key".to_string());
        };
        if chars.next().is_some() {
            return Err("Malformed SCRAM attribute key".to_string());
        }
        attrs.insert(key_char, value.to_string());
    }
    Ok(attrs)
}

fn decode_scram_username(value: &str) -> String {
    value.replace("=2C", ",").replace("=3D", "=")
}

fn derive_scram_secret(
    mechanism: SaslMechanism,
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> (Vec<u8>, Vec<u8>) {
    match mechanism {
        SaslMechanism::ScramSha256 => {
            let mut salted = [0_u8; 32];
            pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut salted);
            let client_key = hmac_sha256(&salted, b"Client Key");
            let stored_key = Sha256::digest(&client_key).to_vec();
            let server_key = hmac_sha256(&salted, b"Server Key");
            (stored_key, server_key)
        }
        SaslMechanism::ScramSha512 => {
            let mut salted = [0_u8; 64];
            pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut salted);
            let client_key = hmac_sha512(&salted, b"Client Key");
            let stored_key = Sha512::digest(&client_key).to_vec();
            let server_key = hmac_sha512(&salted, b"Server Key");
            (stored_key, server_key)
        }
        SaslMechanism::Plain => (Vec::new(), Vec::new()),
    }
}

fn compute_hmac(mechanism: SaslMechanism, key: &[u8], payload: &[u8]) -> Vec<u8> {
    match mechanism {
        SaslMechanism::ScramSha256 => hmac_sha256(key, payload),
        SaslMechanism::ScramSha512 => hmac_sha512(key, payload),
        SaslMechanism::Plain => Vec::new(),
    }
}

fn compute_hash(mechanism: SaslMechanism, payload: &[u8]) -> Vec<u8> {
    match mechanism {
        SaslMechanism::ScramSha256 => Sha256::digest(payload).to_vec(),
        SaslMechanism::ScramSha512 => Sha512::digest(payload).to_vec(),
        SaslMechanism::Plain => Vec::new(),
    }
}

fn hmac_sha256(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC-SHA-256 supports variable key size");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha512(key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha512::new_from_slice(key).expect("HMAC-SHA-512 supports variable key size");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_auth_success_and_failure_paths() {
        let mut config = SaslConfig::new();
        config
            .add_plain_user("alice", "secret")
            .expect("add plain user");

        let mut state = SaslConnectionState::new();
        let handshake = begin_sasl_handshake(&config, "PLAIN", &mut state);
        assert_eq!(handshake.error_code, 0);

        let ok = authenticate_sasl(&config, b"\0alice\0secret", &mut state);
        assert_eq!(ok.error_code, 0);
        assert!(ok.complete);
        assert_eq!(ok.principal.as_deref(), Some("alice"));
        assert!(state.is_authenticated());

        let mut fail_state = SaslConnectionState::new();
        let _ = begin_sasl_handshake(&config, "PLAIN", &mut fail_state);
        let fail = authenticate_sasl(&config, b"\0alice\0wrong", &mut fail_state);
        assert_eq!(fail.error_code, ERROR_SASL_AUTHENTICATION_FAILED);
        assert!(!fail.complete);
    }

    #[test]
    fn scram_sha256_two_step_auth_roundtrip() {
        let mut config = SaslConfig::new();
        config
            .add_scram_sha256_user("bob", "topsecret", b"salt-s256".to_vec(), 4096)
            .expect("add scram user");

        let mut state = SaslConnectionState::new();
        let handshake = begin_sasl_handshake(&config, "SCRAM-SHA-256", &mut state);
        assert_eq!(handshake.error_code, 0);

        let client_first = "n,,n=bob,r=abcdef";
        let first = authenticate_sasl(&config, client_first.as_bytes(), &mut state);
        assert_eq!(first.error_code, 0);
        let server_first = String::from_utf8(first.auth_bytes).expect("utf8 server first");
        assert!(server_first.starts_with("r=abcdef"));

        let (_, bare) = client_first.split_once(",,").expect("bare");
        let client_final_without_proof = format!(
            "c=biws,r={}",
            server_first[2..].split(',').next().expect("nonce")
        );
        let auth_message = format!("{bare},{server_first},{client_final_without_proof}");

        let user = config
            .scram_user(SaslMechanism::ScramSha256, "bob")
            .expect("scram user");
        let client_signature = hmac_sha256(&user.stored_key, auth_message.as_bytes());
        let salted = {
            let mut salted = [0_u8; 32];
            pbkdf2_hmac::<Sha256>(b"topsecret", &user.salt, user.iterations, &mut salted);
            salted
        };
        let client_key = hmac_sha256(&salted, b"Client Key");
        let mut proof = vec![0_u8; client_key.len()];
        for (index, key_byte) in client_key.iter().copied().enumerate() {
            proof[index] = key_byte ^ client_signature[index];
        }
        let client_final = format!(
            "{client_final_without_proof},p={}",
            STANDARD_NO_PAD.encode(proof)
        );
        let final_step = authenticate_sasl(&config, client_final.as_bytes(), &mut state);
        assert_eq!(final_step.error_code, 0);
        assert!(final_step.complete);
        let server_final = String::from_utf8(final_step.auth_bytes).expect("server final");
        assert!(server_final.starts_with("v="));
        assert!(state.is_authenticated());
    }
}
